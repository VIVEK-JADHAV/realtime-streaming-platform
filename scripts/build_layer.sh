#!/bin/bash
# Builds the Lambda layer zip with confluent-kafka and MSK IAM signer.
# Must use Amazon Linux 2023 compatible binaries (Lambda runtime).
# Run: ./scripts/build_layer.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$PROJECT_ROOT/layers"
BUILD_DIR=$(mktemp -d)

echo "Installing dependencies into $BUILD_DIR..."

# Install packages with C extensions (need platform-specific binaries)
pip install \
    --platform manylinux2014_x86_64 \
    --target "$BUILD_DIR/python" \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    confluent-kafka==2.4.0 \
    fastavro==1.9.4

# Install pure-python packages (no binary constraint)
pip install \
    --target "$BUILD_DIR/python" \
    --no-deps \
    aws-msk-iam-sasl-signer-python==1.0.1 \
    "setuptools<70"

echo "Creating zip..."
mkdir -p "$LAYER_DIR"
cd "$BUILD_DIR"
zip -r "$LAYER_DIR/kafka-deps.zip" python/ -x '*.pyc' '*__pycache__*'

rm -rf "$BUILD_DIR"
echo "Layer built: $LAYER_DIR/kafka-deps.zip"
