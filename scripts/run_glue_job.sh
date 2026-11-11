#!/usr/bin/env bash
# ==============================================================================
# AWS Glue (PySpark) Job Execution Runner
# Submits a PySpark ETL job to the Glue local container with Delta Lake and
# S3A LocalStack configurations.
#
# Usage:
#   ./scripts/run_glue_job.sh [job_script_name.py] [extra_args...]
# Example:
#   ./scripts/run_glue_job.sh sample_etl_job.py --source_bucket raw-data-bucket
# ==============================================================================

set -euo pipefail

JOB_NAME="${1:-sample_etl_job.py}"
shift || true
EXTRA_ARGS="$*"

# Resolve project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load .env file if it exists
if [ -f "${PROJECT_ROOT}/.env" ]; then
    # Export variables ignoring comments
    export $(grep -v '^#' "${PROJECT_ROOT}/.env" | xargs -0 2>/dev/null || grep -v '^#' "${PROJECT_ROOT}/.env" | xargs)
fi

AWS_ACCESS_KEY="${AWS_ACCESS_KEY_ID:-mock_access_key}"
AWS_SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-mock_secret_key}"
AWS_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
LOCALSTACK_HOST="${LOCALSTACK_HOSTNAME:-localstack}"
S3_ENDPOINT="http://${LOCALSTACK_HOST}:4566"

echo "============================================================"
echo "[GLUE-RUNNER] Starting AWS Glue PySpark Job: ${JOB_NAME}"
echo "============================================================"
echo "Project Root:      ${PROJECT_ROOT}"
echo "LocalStack S3:     ${S3_ENDPOINT}"
echo "Extra Arguments:   ${EXTRA_ARGS:-None}"
echo "------------------------------------------------------------"

# Determine execution mode: inside container or from host via docker compose
SPARK_SUBMIT_CMD="spark-submit \
    --packages io.delta:delta-core_2.12:2.4.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
    --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY} \
    --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_KEY} \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /home/glue_user/workspace/jupyter_workspace/${JOB_NAME} ${EXTRA_ARGS}"

if command -v spark-submit &> /dev/null && [ -d "/home/glue_user" ]; then
    echo "[GLUE-RUNNER] Running directly inside Glue container environment..."
    eval "$SPARK_SUBMIT_CMD"
else
    echo "[GLUE-RUNNER] Delegating execution to 'glue-runner' Docker container..."
    
    # Check if docker compose is available
    if command -v docker &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
        if ! docker compose version &> /dev/null; then
            DOCKER_COMPOSE_CMD="docker-compose"
        fi

        # Execute spark-submit inside running glue-runner container
        cd "${PROJECT_ROOT}"
        $DOCKER_COMPOSE_CMD exec -T glue-runner bash -c "$SPARK_SUBMIT_CMD"
    else
        echo "[ERROR] Docker is not installed or not in PATH."
        exit 1
    fi
fi

echo "============================================================"
echo "[GLUE-RUNNER] Job execution finished successfully."
echo "============================================================"

