#!/usr/bin/env bash
# ==============================================================================
# Upload Sample Input Data to Amazon S3
# Uploads local sample e-commerce records to your real AWS S3 raw data bucket.
#
# Usage:
#   ./scripts/upload_sample_data_to_s3.sh [BUCKET_NAME] [AWS_PROFILE] [AWS_REGION]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load .env file if it exists
if [ -f "${PROJECT_ROOT}/.env" ]; then
    export $(grep -v '^#' "${PROJECT_ROOT}/.env" | xargs -0 2>/dev/null || grep -v '^#' "${PROJECT_ROOT}/.env" | xargs)
fi

BUCKET_NAME="${1:-${S3_RAW_BUCKET:-${S3_PROCESSED_BUCKET:-}}}"
PROFILE="${2:-${AWS_PROFILE:-giduturi-tutorial-profile}}"
REGION="${3:-${AWS_DEFAULT_REGION:-ap-south-1}}"

if [ -z "$BUCKET_NAME" ] || [ "$BUCKET_NAME" == "your-actual-aws-s3-bucket-name" ]; then
    echo "============================================================"
    echo "[ERROR] Please specify your actual AWS S3 bucket name."
    echo "Usage: $0 <S3_BUCKET_NAME> [AWS_PROFILE] [AWS_REGION]"
    echo "Example: $0 my-real-s3-bucket giduturi-tutorial-profile ap-south-1"
    echo "============================================================"
    exit 1
fi

SAMPLE_FILE="${PROJECT_ROOT}/local_data/raw/sample_input.csv"

if [ ! -f "$SAMPLE_FILE" ]; then
    echo "[ERROR] Sample data file not found at: $SAMPLE_FILE"
    exit 1
fi

echo "============================================================"
echo "[S3-UPLOAD] Uploading sample input data to Amazon S3..."
echo "Source File:  $SAMPLE_FILE"
echo "Destination:  s3://$BUCKET_NAME/raw/sample_input.csv"
echo "AWS Profile:  $PROFILE"
echo "AWS Region:   $REGION"
echo "============================================================"

AWS="aws --profile $PROFILE --region $REGION"

# Ensure bucket exists
if ! $AWS s3 ls "s3://$BUCKET_NAME" &>/dev/null; then
    echo "[WARN] Bucket s3://$BUCKET_NAME not reachable. Checking credentials..."
fi

$AWS s3 cp "$SAMPLE_FILE" "s3://$BUCKET_NAME/raw/sample_input.csv"
echo "[SUCCESS] Successfully uploaded sample input data to s3://$BUCKET_NAME/raw/sample_input.csv"
$AWS s3 ls "s3://$BUCKET_NAME/raw/" --human-readable

