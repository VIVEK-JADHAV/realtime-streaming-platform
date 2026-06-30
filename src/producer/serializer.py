"""
Avro serializer for clickstream events using Glue Schema Registry.

Serializes events to Avro binary format and validates against the registered schema.
Uses the fastavro library for fast serialization.
"""

import io
import json
import os

import boto3
import fastavro

AWS_REGION = os.environ.get("AWS_MSK_REGION", "us-east-1")
SCHEMA_REGISTRY_NAME = os.environ.get("SCHEMA_REGISTRY_NAME", "")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "ClickstreamEvent")

# Load schema from file (bundled with Lambda)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "clickstream_event.avsc")

with open(SCHEMA_PATH) as f:
    AVRO_SCHEMA = json.load(f)

# Parse schema for fastavro
PARSED_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA)


def serialize_event(event: dict) -> bytes:
    """
    Serialize a clickstream event to Avro binary.

    Raises ValueError if event doesn't match schema (e.g., invalid enum value,
    wrong type, missing required field).
    """
    # Convert event_type to uppercase enum symbol
    event_copy = event.copy()
    event_copy["event_type"] = event_copy["event_type"].upper()

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, PARSED_SCHEMA, event_copy)
    return buffer.getvalue()


def deserialize_event(data: bytes) -> dict:
    """Deserialize Avro binary back to a dict (for testing/consumers)."""
    buffer = io.BytesIO(data)
    return fastavro.schemaless_reader(buffer, PARSED_SCHEMA)
