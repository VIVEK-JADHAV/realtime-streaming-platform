"""
Topic administration utilities.
Creates the clickstream topic if it doesn't exist.
"""

import os

from confluent_kafka.admin import AdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


AWS_REGION = os.environ.get("AWS_MSK_REGION", "us-east-1")


def _oauth_cb(config_str):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return auth_token, expiry_ms / 1000


def get_admin_config(bootstrap_servers: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "sasl.oauthbearer.config": "unused",
        "oauth_cb": _oauth_cb,
        "broker.address.family": "v4",
    }


def ensure_topic_exists(bootstrap_servers: str, topic_name: str, num_partitions: int = 12):
    """Create topic if it doesn't already exist. Idempotent."""
    admin = AdminClient(get_admin_config(bootstrap_servers))

    # Poll to trigger oauth_cb (required in confluent-kafka < 2.13)
    admin.poll(10.0)

    # Check existing topics
    metadata = admin.list_topics(timeout=10)
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=3,
        config={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "delete",
        },
    )

    futures = admin.create_topics([topic])
    for name, future in futures.items():
        try:
            future.result()
            print(f"Topic '{name}' created with {num_partitions} partitions.")
        except Exception as e:
            if "TopicExistsException" in str(type(e).__name__) or "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"Topic '{name}' already exists (race condition, safe to ignore).")
            else:
                raise
