"""
Clickstream event producer for ShopStream.

Generates realistic e-commerce clickstream events and produces them to MSK.
Invoked by Lambda on a schedule (every minute), producing 1500-2000 events per invocation.

Producer config (from Day 2 videos):
- acks=all: Wait for all in-sync replicas to acknowledge (max durability)
- enable.idempotence=true: Prevents duplicates on retry (sequence numbers per partition)
- compression.type=snappy: Fast compression, good for JSON/text-heavy payloads
- linger.ms=20: Wait up to 20ms to batch messages (throughput vs latency tradeoff)
- batch.size=65536: 64KB batches (larger = better compression + fewer requests)
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from topic_admin import ensure_topic_exists
from serializer import serialize_event


# --- Configuration ---

MSK_CLUSTER_ARN = os.environ["MSK_CLUSTER_ARN"]
TOPIC_NAME = os.environ.get("TOPIC_NAME", "shopstream.clickstream")
AWS_REGION = os.environ.get("AWS_MSK_REGION", "us-east-1")

# Resolve bootstrap brokers once per Lambda cold start
import boto3

_kafka_client = boto3.client("kafka", region_name=AWS_REGION)
_brokers_response = _kafka_client.get_bootstrap_brokers(ClusterArn=MSK_CLUSTER_ARN)
print(f"Brokers response: {_brokers_response}")
BOOTSTRAP_SERVERS = _brokers_response.get("BootstrapBrokerStringSaslIam", "")
print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")

# Event type distribution (realistic e-commerce funnel)
EVENT_WEIGHTS = {
    "page_view": 70,
    "search": 15,
    "add_to_cart": 10,
    "purchase": 5,
}

EVENT_TYPES = list(EVENT_WEIGHTS.keys())
EVENT_PROBABILITIES = [w / 100 for w in EVENT_WEIGHTS.values()]

# Realistic product categories
CATEGORIES = [
    "electronics", "clothing", "home_garden", "books",
    "sports", "beauty", "toys", "automotive",
]

# Device distribution
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.40, 0.50, 0.10]

# Page URLs by event type
PAGE_TEMPLATES = {
    "page_view": [
        "/products/{product_id}",
        "/category/{category}",
        "/",
        "/deals",
        "/account/orders",
    ],
    "search": ["/search?q={query}"],
    "add_to_cart": ["/products/{product_id}"],
    "purchase": ["/checkout/confirm"],
}

SEARCH_QUERIES = [
    "wireless headphones", "running shoes", "laptop stand",
    "water bottle", "yoga mat", "phone case", "backpack",
    "desk lamp", "bluetooth speaker", "kitchen knife set",
]

REFERRERS = [
    "https://www.google.com", "https://www.google.com",  # weighted toward google
    "https://www.facebook.com", "https://email.shopstream.com",
    "direct", "direct", "direct",
    "https://www.instagram.com", "https://www.tiktok.com",
]


def _oauth_cb(config_str):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return auth_token, expiry_ms / 1000


def get_producer_config() -> dict:
    """MSK IAM auth + production producer settings."""
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "sasl.oauthbearer.config": "unused",
        "oauth_cb": _oauth_cb,
        "broker.address.family": "v4",
        # --- Production settings (from Day 2 videos) ---
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 2147483647,
        "compression.type": "snappy",
        "linger.ms": 20,
        "batch.size": 65536,
    }


def generate_event(batch_time: datetime) -> dict:
    """Generate a single realistic clickstream event."""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_PROBABILITIES, k=1)[0]
    session_id = str(uuid.uuid4())[:12]
    user_id = f"user_{random.randint(1, 200000)}" if random.random() > 0.15 else None
    product_id = f"prod_{random.randint(1, 500000)}" if event_type != "search" else None
    category = random.choice(CATEGORIES)
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]

    # Build page URL
    page_templates = PAGE_TEMPLATES[event_type]
    page_url = random.choice(page_templates).format(
        product_id=product_id or "unknown",
        category=category,
        query=random.choice(SEARCH_QUERIES),
    )

    # Slight timestamp jitter within the batch (events don't all happen at the same ms)
    jitter_ms = random.randint(0, 59000)
    event_time = int(batch_time.timestamp() * 1000) + jitter_ms

    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product_id,
        "category": category,
        "timestamp": event_time,
        "page_url": page_url,
        "referrer": random.choice(REFERRERS),
        "device_type": device,
        "properties": {
            "viewport_width": str(random.choice([375, 768, 1024, 1440, 1920])),
            "page_load_ms": str(random.randint(200, 3000)),
        },
    }


# Track delivery stats
delivery_stats = {"success": 0, "error": 0, "partitions": {}}
# DLQ buffer for failed events
dlq_buffer = []


def delivery_callback(err, msg):
    """Called once per message to confirm delivery or log failure."""
    if err:
        delivery_stats["error"] += 1
        dlq_buffer.append({
            "error": str(err),
            "topic": msg.topic(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value_size": len(msg.value()) if msg.value() else 0,
        })
    else:
        delivery_stats["success"] += 1
        partition = msg.partition()
        delivery_stats["partitions"][partition] = delivery_stats["partitions"].get(partition, 0) + 1


def publish_partition_metrics(partitions: dict, batch_time: datetime):
    """Publish per-partition message count to CloudWatch for balance monitoring."""
    if not partitions:
        return

    cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
    metric_data = []

    for partition_id, count in partitions.items():
        metric_data.append({
            "MetricName": "MessagesPerPartition",
            "Dimensions": [
                {"Name": "Topic", "Value": TOPIC_NAME},
                {"Name": "Partition", "Value": str(partition_id)},
            ],
            "Timestamp": batch_time,
            "Value": count,
            "Unit": "Count",
        })

    # CloudWatch accepts max 1000 metrics per call
    for i in range(0, len(metric_data), 1000):
        cloudwatch.put_metric_data(
            Namespace="ShopStream/Kafka",
            MetricData=metric_data[i:i+1000],
        )


def flush_dlq_to_s3(dlq_events: list, batch_time: datetime):
    """Write failed events to S3 dead-letter path."""
    if not dlq_events:
        return

    s3 = boto3.client("s3", region_name=AWS_REGION)
    dlq_key = (
        f"dlq/clickstream/"
        f"year={batch_time.year}/"
        f"month={batch_time.month:02d}/"
        f"day={batch_time.day:02d}/"
        f"hour={batch_time.hour:02d}/"
        f"{batch_time.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
    )

    body = json.dumps({"failed_events": dlq_events, "count": len(dlq_events)})
    s3.put_object(
        Bucket=os.environ.get("DLQ_BUCKET", f"shopstream-raw-{AWS_REGION}"),
        Key=dlq_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"DLQ: wrote {len(dlq_events)} failed events to s3://{dlq_key}")


def lambda_handler(event, context):
    """
    Produce 1500-2000 clickstream events to MSK.
    Invoked every minute by EventBridge schedule.
    """
    # Ensure topic exists on first invocation (idempotent)
    ensure_topic_exists(BOOTSTRAP_SERVERS, TOPIC_NAME, num_partitions=12)

    batch_size = random.randint(1500, 2000)
    batch_time = datetime.now(timezone.utc)
    delivery_stats["success"] = 0
    delivery_stats["error"] = 0
    delivery_stats["partitions"] = {}
    dlq_buffer.clear()

    producer = Producer(get_producer_config())
    # Poll to trigger oauth_cb (required in confluent-kafka < 2.13)
    producer.poll(10.0)

    for _ in range(batch_size):
        evt = generate_event(batch_time)

        # Serialize to Avro binary (validates against schema — rejects bad data)
        avro_bytes = serialize_event(evt)

        # Partition key = session_id (ordering guarantee per session)
        producer.produce(
            topic=TOPIC_NAME,
            key=evt["session_id"],
            value=avro_bytes,
            callback=delivery_callback,
        )

        # Serve delivery callbacks periodically (avoid memory buildup)
        producer.poll(0)

    # Flush: block until all messages are delivered or fail
    remaining = producer.flush(timeout=30)

    # Publish partition distribution to CloudWatch
    publish_partition_metrics(delivery_stats["partitions"], batch_time)

    # Write any failed events to S3 DLQ
    flush_dlq_to_s3(dlq_buffer, batch_time)

    result = {
        "batch_size": batch_size,
        "delivered": delivery_stats["success"],
        "errors": delivery_stats["error"],
        "unflushed": remaining,
        "partition_distribution": delivery_stats["partitions"],
        "dlq_count": len(dlq_buffer),
        "timestamp": batch_time.isoformat(),
    }

    print(json.dumps(result))
    return result
