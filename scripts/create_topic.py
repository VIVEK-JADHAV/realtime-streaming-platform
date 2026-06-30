"""
Create the shopstream.clickstream topic on MSK Serverless.

Usage:
    python scripts/create_topic.py --bootstrap-servers <brokers>

Requires: confluent-kafka[avro] package and AWS IAM auth via MSK IAM SASL.
Run from a machine that has network access to the MSK cluster (e.g., an EC2 in the same VPC,
or via VPN/SSM session).
"""

import argparse
import json
import socket

from confluent_kafka.admin import AdminClient, NewTopic


def get_msk_config(bootstrap_servers: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "sasl.oauthbearer.method": "oidc",
        "sasl.oauthbearer.client.id": "client1",
        "sasl.oauthbearer.token.endpoint.url": "",
        # For MSK IAM, use the aws_msk_iam_sasl_signer library instead.
        # This config is a placeholder — see README for actual auth setup.
    }


def get_msk_iam_config(bootstrap_servers: str) -> dict:
    """MSK IAM auth config using aws-msk-iam-sasl-signer-python."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def oauth_cb(oauth_config):
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return auth_token, expiry_ms / 1000

    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
    }


def create_topic(bootstrap_servers: str, topic_name: str, num_partitions: int):
    config = get_msk_iam_config(bootstrap_servers)
    admin = AdminClient(config)

    topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=3,  # MSK Serverless manages this, but API requires it
        config={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "delete",
        },
    )

    print(f"Creating topic '{topic_name}' with {num_partitions} partitions...")
    futures = admin.create_topics([topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic_name}': {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topic on MSK Serverless")
    parser.add_argument("--bootstrap-servers", required=True, help="MSK bootstrap brokers")
    parser.add_argument("--topic", default="shopstream.clickstream")
    parser.add_argument("--partitions", type=int, default=12)
    args = parser.parse_args()

    create_topic(args.bootstrap_servers, args.topic, args.partitions)
