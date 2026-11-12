#!/usr/bin/env bash
# ==============================================================================
# AWS CLI Automated Setup for Glue Data Catalog, Crawler & Amazon Athena
# Provisons required AWS cloud resources for Approach B:
#   1. AWS Glue Catalog Database
#   2. IAM Service Role & Policies for Glue Crawler
#   3. AWS Glue Crawler pointing to S3 Parquet dataset
#   4. Athena Workgroup with query results output location
# ==============================================================================

set -euo pipefail

PROFILE="${1:-${AWS_PROFILE:-giduturi-tutorial-profile}}"
REGION="${2:-${AWS_DEFAULT_REGION:-ap-south-1}}"
BUCKET_NAME="${3:-}"

if [ -z "$BUCKET_NAME" ]; then
    echo "============================================================"
    echo "Usage: $0 [AWS_PROFILE] [AWS_REGION] <S3_BUCKET_NAME>"
    echo "Example: $0 giduturi-tutorial-profile ap-south-1 my-ecommerce-data-bucket-12345"
    echo "============================================================"
    exit 1
fi

DATABASE_NAME="${GLUE_DATABASE_NAME:-ecommerce_db}"
CRAWLER_NAME="${GLUE_CRAWLER_NAME:-ecommerce_orders_crawler}"
ROLE_NAME="ecommerce-glue-crawler-service-role"
WORKGROUP_NAME="ecommerce_analytics"

echo "============================================================"
echo "[AWS-SETUP] Provisioning Glue Catalog, Crawler & Athena..."
echo "Profile:     $PROFILE"
echo "Region:      $REGION"
echo "Bucket:      s3://$BUCKET_NAME"
echo "Database:    $DATABASE_NAME"
echo "Crawler:     $CRAWLER_NAME"
echo "============================================================"

AWS="aws --profile $PROFILE --region $REGION"

# 1. Create S3 Bucket if it doesn't already exist
echo "[1/5] Checking S3 Bucket s3://$BUCKET_NAME..."
if ! $AWS s3 ls "s3://$BUCKET_NAME" 2>/dev/null; then
    echo "Creating S3 bucket: s3://$BUCKET_NAME in $REGION..."
    if [ "$REGION" == "us-east-1" ]; then
        $AWS s3 mb "s3://$BUCKET_NAME"
    else
        $AWS s3 mb "s3://$BUCKET_NAME" --create-bucket-configuration LocationConstraint="$REGION"
    fi
else
    echo "Bucket s3://$BUCKET_NAME already exists."
fi

# Ensure necessary folder prefixes exist
$AWS s3api put-object --bucket "$BUCKET_NAME" --key "orders_processed/" >/dev/null || true
$AWS s3api put-object --bucket "$BUCKET_NAME" --key "athena_query_results/" >/dev/null || true

# 2. Create Glue Catalog Database
echo "[2/5] Creating Glue Catalog Database: $DATABASE_NAME..."
$AWS glue create-database --database-input "{\"Name\":\"$DATABASE_NAME\",\"Description\":\"Database for ecommerce data pipeline\"}" 2>/dev/null || echo "Database $DATABASE_NAME already exists."

# 3. Create IAM Role for Glue Crawler
echo "[3/5] Setting up IAM Role: $ROLE_NAME..."
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "glue.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

ROLE_ARN=$($AWS iam get-role --role-name "$ROLE_NAME" --query "Role.Arn" --output text 2>/dev/null || true)
if [ -z "$ROLE_ARN" ]; then
    echo "Creating IAM Role: $ROLE_NAME..."
    ROLE_ARN=$($AWS iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "$TRUST_POLICY" --query "Role.Arn" --output text)
    $AWS iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

    S3_POLICY="{
      \"Version\": \"2012-10-17\",
      \"Statement\": [
        {
          \"Effect\": \"Allow\",
          \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:ListBucket\"],
          \"Resource\": [\"arn:aws:s3:::$BUCKET_NAME\", \"arn:aws:s3:::$BUCKET_NAME/*\"]
        }
      ]
    }"
    $AWS iam put-role-policy --role-name "$ROLE_NAME" --policy-name "GlueS3AccessPolicy" --policy-document "$S3_POLICY"
    echo "Waiting 10s for IAM propagation..."
    sleep 10
else
    echo "IAM Role $ROLE_NAME exists with ARN: $ROLE_ARN"
fi

# 4. Create AWS Glue Crawler
echo "[4/5] Creating AWS Glue Crawler: $CRAWLER_NAME..."
if ! $AWS glue get-crawler --name "$CRAWLER_NAME" 2>/dev/null; then
    $AWS glue create-crawler \
        --name "$CRAWLER_NAME" \
        --role "$ROLE_ARN" \
        --database-name "$DATABASE_NAME" \
        --description "Crawler for e-commerce orders Parquet dataset" \
        --targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET_NAME/orders_processed/\"}]}" \
        --schema-change-policy "UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG"
    echo "Created Crawler $CRAWLER_NAME."
else
    echo "Crawler $CRAWLER_NAME already exists."
fi

# 5. Create Athena Workgroup
echo "[5/5] Creating Athena Workgroup: $WORKGROUP_NAME..."
if ! $AWS athena get-work-group --work-group "$WORKGROUP_NAME" 2>/dev/null; then
    $AWS athena create-work-group \
        --name "$WORKGROUP_NAME" \
        --configuration "ResultConfiguration={OutputLocation=s3://$BUCKET_NAME/athena_query_results/}" \
        --description "Dedicated workgroup for ecommerce analytics"
    echo "Created Athena Workgroup $WORKGROUP_NAME."
else
    echo "Athena Workgroup $WORKGROUP_NAME already exists."
fi

echo "============================================================"
echo "[SUCCESS] Cloud setup complete! Resources ready:"
echo "  - S3 Data Bucket:     s3://$BUCKET_NAME"
echo "  - Glue Database:      $DATABASE_NAME"
echo "  - Glue Crawler:       $CRAWLER_NAME"
echo "  - Athena Workgroup:   $WORKGROUP_NAME"
echo "============================================================"

