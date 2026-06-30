#!/bin/bash
# Load test: invoke producer Lambda 100 times concurrently.
# Simulates traffic spike of ~150K-200K events.
#
# Usage: ./scripts/load_test.sh [region]

set -euo pipefail

REGION="${1:-us-east-1}"
FUNCTION_NAME="shopstream-dev-clickstream-producer"
CONCURRENT=50
OUTPUT_DIR=$(mktemp -d)

echo "Load test: invoking $FUNCTION_NAME $CONCURRENT times concurrently..."
echo "Region: $REGION"
echo "Output: $OUTPUT_DIR"
echo ""

# Launch all invocations in parallel
for i in $(seq 1 $CONCURRENT); do
    aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --region "$REGION" \
        --invocation-type "RequestResponse" \
        "$OUTPUT_DIR/result_$i.json" \
        > /dev/null 2>&1 &
done

echo "Waiting for all $CONCURRENT invocations to complete..."
wait
echo ""

# Aggregate results
total_produced=0
total_delivered=0
total_errors=0
total_unflushed=0
failures=0

for f in "$OUTPUT_DIR"/result_*.json; do
    if [ -s "$f" ]; then
        delivered=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('delivered', 0))" 2>/dev/null || echo 0)
        errors=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('errors', 0))" 2>/dev/null || echo 0)
        batch=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('batch_size', 0))" 2>/dev/null || echo 0)
        unflushed=$(python3 -c "import json; d=json.load(open('$f')); print(d.get('unflushed', 0))" 2>/dev/null || echo 0)

        total_produced=$((total_produced + batch))
        total_delivered=$((total_delivered + delivered))
        total_errors=$((total_errors + errors))
        total_unflushed=$((total_unflushed + unflushed))
    else
        failures=$((failures + 1))
    fi
done

echo "========================================="
echo "LOAD TEST RESULTS"
echo "========================================="
echo "Invocations:    $CONCURRENT"
echo "Failed invoke:  $failures"
echo "Total produced: $total_produced"
echo "Total delivered: $total_delivered"
echo "Total errors:   $total_errors"
echo "Total unflushed: $total_unflushed"
echo "========================================="

if [ $total_errors -eq 0 ] && [ $total_unflushed -eq 0 ] && [ $failures -eq 0 ]; then
    echo "✓ PASS: All events delivered successfully"
else
    echo "✗ FAIL: Some events were not delivered"
fi

# Cleanup
rm -rf "$OUTPUT_DIR"
