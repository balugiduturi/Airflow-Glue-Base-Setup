#!/usr/bin/env bash
# ==============================================================================
# LocalStack S3 Initialization Script
# Creates required S3 buckets and seeds the raw input data.
# Executed automatically by LocalStack via /etc/localstack/init/ready.d/
# ==============================================================================

set -euo pipefail

echo "============================================================"
echo "[INIT] Initializing LocalStack S3 Buckets and Seed Data..."
echo "============================================================"

# Choose AWS CLI wrapper: awslocal inside container, or aws with endpoint fallback
if command -v awslocal &> /dev/null; then
    AWS_CMD="awslocal"
elif command -v aws &> /dev/null; then
    ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
    AWS_CMD="aws --endpoint-url=${ENDPOINT}"
else
    echo "[ERROR] Neither awslocal nor aws CLI found in PATH."
    exit 1
fi

BUCKET_RAW="${S3_RAW_BUCKET:-raw-data-bucket}"
BUCKET_PROCESSED="${S3_PROCESSED_BUCKET:-processed-data-bucket}"
BUCKET_CURATED="${S3_CURATED_BUCKET:-curated-data-bucket}"

create_bucket_if_not_exists() {
    local bucket_name="$1"
    echo "[INIT] Checking bucket: s3://${bucket_name}"
    if ! $AWS_CMD s3 ls "s3://${bucket_name}" 2>/dev/null; then
        echo "[INIT] Creating bucket: s3://${bucket_name}"
        $AWS_CMD s3 mb "s3://${bucket_name}"
    else
        echo "[INIT] Bucket s3://${bucket_name} already exists."
    fi
}

# 1. Create S3 Buckets
create_bucket_if_not_exists "$BUCKET_RAW"
create_bucket_if_not_exists "$BUCKET_PROCESSED"
create_bucket_if_not_exists "$BUCKET_CURATED"

echo "------------------------------------------------------------"
echo "[INIT] Current S3 Buckets in LocalStack:"
$AWS_CMD s3 ls
echo "------------------------------------------------------------"

# 2. Locate and sync sample_input.csv
SAMPLE_FILE=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "/tmp/local_data/raw/sample_input.csv" ]; then
    SAMPLE_FILE="/tmp/local_data/raw/sample_input.csv"
elif [ -f "${SCRIPT_DIR}/../local_data/raw/sample_input.csv" ]; then
    SAMPLE_FILE="${SCRIPT_DIR}/../local_data/raw/sample_input.csv"
elif [ -f "./local_data/raw/sample_input.csv" ]; then
    SAMPLE_FILE="./local_data/raw/sample_input.csv"
fi

if [ -n "$SAMPLE_FILE" ] && [ -f "$SAMPLE_FILE" ]; then
    echo "[INIT] Seeding mock data from: $SAMPLE_FILE"
    $AWS_CMD s3 cp "$SAMPLE_FILE" "s3://${BUCKET_RAW}/raw/sample_input.csv"
    $AWS_CMD s3 cp "$SAMPLE_FILE" "s3://${BUCKET_RAW}/sample_input.csv"
    echo "[INIT] Successfully uploaded sample_input.csv to s3://${BUCKET_RAW}/"
    echo "[INIT] Contents of s3://${BUCKET_RAW}/:"
    $AWS_CMD s3 ls "s3://${BUCKET_RAW}/" --recursive
else
    echo "[WARN] sample_input.csv not found at standard search paths. Skipping auto-seed."
fi

echo "============================================================"
echo "[INIT] LocalStack S3 Initialization Complete!"
echo "============================================================"

