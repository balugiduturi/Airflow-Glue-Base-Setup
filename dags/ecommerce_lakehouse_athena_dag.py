"""
Enterprise Lakehouse Pipeline with AWS Glue Catalog, Crawler & Amazon Athena
----------------------------------------------------------------------------
Implements Approach B (Real AWS Cloud Hybrid Pipeline):
1. Pre-flight verification of AWS connectivity and S3 storage.
2. Executes PySpark ETL job to output partitioned Parquet orders.
3. Triggers AWS Glue Crawler via GlueCrawlerOperator to scan Parquet files.
4. Waits for Crawler completion using GlueCrawlerSensor (populates Glue Data Catalog).
5. Executes analytical SQL aggregations using Amazon Athena (AthenaOperator).
6. Reads and logs analytical KPIs from the generated Athena query result.
"""

from datetime import datetime, timedelta
import os
import time
import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# Environment configuration
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "giduturi-tutorial-profile")
S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "raw-data-bucket")
GLUE_DATABASE_NAME = os.environ.get("GLUE_DATABASE_NAME", "ecommerce_db")
GLUE_CRAWLER_NAME = os.environ.get("GLUE_CRAWLER_NAME", "ecommerce_orders_crawler")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT_S3 = os.environ.get(
    "ATHENA_OUTPUT_LOCATION",
    f"s3://{S3_PROCESSED_BUCKET}/athena_query_results/",
)

# Airflow task default parameters
default_args = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def pre_flight_aws_check(**kwargs):
    """
    Validates AWS credentials and verifies that the target S3 bucket and Glue database are reachable.
    """
    session = boto3.Session(region_name=AWS_REGION)
    s3 = session.client("s3")
    glue = session.client("glue")

    print(f"[AWS-CHECK] Verifying connectivity to AWS region: {AWS_REGION}...")

    # Check S3 Bucket access
    try:
        s3.head_bucket(Bucket=S3_PROCESSED_BUCKET)
        print(f"[AWS-CHECK] S3 Bucket 's3://{S3_PROCESSED_BUCKET}' is accessible.")
    except Exception as e:
        print(f"[AWS-WARN] S3 Bucket check note: {e}")

    # Check Glue Database
    try:
        db = glue.get_database(Name=GLUE_DATABASE_NAME)
        print(f"[AWS-CHECK] Glue Database '{GLUE_DATABASE_NAME}' confirmed: {db['Database']['Name']}")
    except Exception as e:
        print(f"[AWS-WARN] Glue Database note: {e}. Will be auto-created if Crawler runs.")


def inspect_athena_results(**kwargs):
    """
    Fetches the latest Athena analytical query results and displays the business KPIs.
    """
    ti = kwargs["ti"]
    query_execution_id = ti.xcom_pull(task_ids="execute_athena_sales_analysis")

    if not query_execution_id:
        print("[ATHENA] No query execution ID found in XCom.")
        return

    print(f"[ATHENA] Inspecting results for QueryExecutionId: {query_execution_id}...")
    session = boto3.Session(region_name=AWS_REGION)
    athena = session.client("athena")

    # Fetch query results
    response = athena.get_query_results(QueryExecutionId=query_execution_id)
    rows = response.get("ResultSet", {}).get("Rows", [])

    print("============================================================")
    print("           AMAZON ATHENA BUSINESS ANALYTICS REPORT           ")
    print("============================================================")
    for row in rows:
        row_values = [col.get("VarCharValue", "NULL") for col in row.get("Data", [])]
        print(" | ".join(f"{val:>20}" for val in row_values))
    print("============================================================")


# Analytical query executed on the crawled Glue Catalog Table
ATHENA_ANALYTICAL_QUERY = f"""
SELECT 
    product_category,
    COUNT(order_id) AS total_completed_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(total_amount), 2) AS average_order_value_usd,
    ROUND(MAX(total_amount), 2) AS highest_order_value_usd
FROM "{GLUE_DATABASE_NAME}"."orders_processed"
GROUP BY product_category
ORDER BY total_revenue_usd DESC;
"""

with DAG(
    dag_id="ecommerce_lakehouse_athena_dag",
    default_args=default_args,
    description="End-to-end Lakehouse pipeline: PySpark ETL -> Glue Crawler -> Glue Catalog -> Athena SQL",
    schedule_interval=None,  # Trigger on-demand
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aws", "glue", "crawler", "catalog", "athena", "lakehouse"],
) as dag:

    # 1. AWS Pre-flight check
    check_aws_connectivity = PythonOperator(
        task_id="check_aws_connectivity",
        python_callable=pre_flight_aws_check,
    )

    # 2. PySpark ETL: Generates Partitioned Parquet Data in S3
    run_glue_pyspark_etl = BashOperator(
        task_id="run_glue_pyspark_etl",
        bash_command="""
            echo "[PIPELINE] Executing PySpark ETL to write Parquet dataset..."
            if command -v docker &> /dev/null && [ -S /var/run/docker.sock ]; then
                docker exec de_base_workspace-glue-runner spark-submit \
                    /home/glue_user/workspace/jupyter_workspace/sample_etl_job.py \
                    --target_path s3a://${S3_PROCESSED_BUCKET}/orders_processed/
            else
                python /opt/airflow/glue_jobs/sample_etl_job.py \
                    --target_path s3://${S3_PROCESSED_BUCKET}/orders_processed/
            fi
        """,
    )

    # 3. Trigger AWS Glue Crawler
    trigger_crawler = GlueCrawlerOperator(
        task_id="trigger_glue_crawler",
        config={"Name": GLUE_CRAWLER_NAME},
    )

    # 4. Wait for Crawler to populate Glue Data Catalog
    wait_for_crawler = GlueCrawlerSensor(
        task_id="wait_for_glue_crawler",
        crawler_name=GLUE_CRAWLER_NAME,
        poll_interval=15,
        timeout=600,
    )

    # 5. Run Analytical SQL on Glue Catalog Table via Amazon Athena
    run_athena_analytics = AthenaOperator(
        task_id="execute_athena_sales_analysis",
        query=ATHENA_ANALYTICAL_QUERY,
        database=GLUE_DATABASE_NAME,
        output_location=ATHENA_OUTPUT_S3,
        workgroup=ATHENA_WORKGROUP,
    )

    # 6. Fetch & Log Analytical Insights
    fetch_athena_report = PythonOperator(
        task_id="fetch_athena_report",
        python_callable=inspect_athena_results,
    )

    # Pipeline task dependencies
    (
        check_aws_connectivity
        >> run_glue_pyspark_etl
        >> trigger_crawler
        >> wait_for_crawler
        >> run_athena_analytics
        >> fetch_athena_report
    )

