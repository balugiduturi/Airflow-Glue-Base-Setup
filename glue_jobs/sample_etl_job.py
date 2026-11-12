"""
Sample AWS Glue (PySpark) ETL Job
---------------------------------
Demonstrates a production-grade PySpark ETL pipeline for AWS Glue and LocalStack:
1. Extracts transactional e-commerce orders from CSV (LocalStack S3 or local storage).
2. Applies data transformations:
   - Filters for valid, COMPLETED orders with quantity > 0.
   - Converts string columns (customer_name, product_category, order_status) to UPPERCASE.
   - Calculates and verifies total_amount.
   - Enriches data with ingestion audit timestamps.
3. Loads curated results partitioned by product_category into Parquet format.
"""

import sys
import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SampleGlueETL")

# Schema definition for source data
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", StringType(), True),
])


def get_spark_session(app_name: str = "SampleGlueETLJob") -> SparkSession:
    """
    Initializes and returns a SparkSession configured with Delta Lake and AWS S3A support.
    Uses AWS DefaultAWSCredentialsProviderChain to authenticate via mounted ~/.aws or IAM roles.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return builder.getOrCreate()


def extract_data(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Extracts raw CSV orders data from S3 or local filesystem.
    """
    logger.info(f"Extracting source data from path: {source_path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(ORDER_SCHEMA)
        .load(source_path)
    )
    logger.info(f"Successfully extracted {df.count()} records.")
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """
    Applies business transformations:
    - Filters records where status is COMPLETED and quantity > 0.
    - Formats strings (customer_name, product_category, order_status) to uppercase and trimmed.
    - Recalculates calculated_total = round(quantity * unit_price, 2).
    - Adds an audit timestamp column `processed_timestamp`.
    """
    logger.info("Applying transformations on orders DataFrame...")

    transformed_df = (
        df.filter(
            (F.upper(F.trim(F.col("order_status"))) == "COMPLETED")
            & (F.col("quantity") > 0)
        )
        .withColumn("customer_name", F.upper(F.trim(F.col("customer_name"))))
        .withColumn("product_category", F.upper(F.trim(F.col("product_category"))))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn(
            "total_amount",
            F.round(F.col("quantity").cast("double") * F.col("unit_price"), 2),
        )
        .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("processed_timestamp", F.current_timestamp())
    )

    return transformed_df


def load_data(
    df: DataFrame,
    target_path: str,
    output_format: str = "parquet",
    partition_col: Optional[str] = "product_category",
    mode: str = "overwrite",
) -> None:
    """
    Writes transformed DataFrame to target path in Parquet (or Delta) format.
    """
    logger.info(f"Writing transformed dataset to {target_path} (Format: {output_format}, Mode: {mode})...")

    writer = df.write.mode(mode).format(output_format)
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)

    writer.save(target_path)
    logger.info("Data write completed successfully.")


def parse_args():
    """
    Parses CLI arguments compatible with both standard python/spark-submit and AWS Glue.
    """
    parser = argparse.ArgumentParser(description="AWS Glue PySpark ETL Job")
    parser.add_argument(
        "--source_path",
        default="s3a://raw-data-bucket/raw/sample_input.csv",
        help="S3 or local path to the raw input CSV file",
    )
    parser.add_argument(
        "--target_path",
        default="s3a://processed-data-bucket/orders_processed/",
        help="S3 or local path to output processed Parquet dataset",
    )
    parser.add_argument(
        "--output_format",
        default="parquet",
        choices=["parquet", "delta"],
        help="Output storage format (parquet or delta)",
    )
    parser.add_argument(
        "--local_fallback",
        action="store_true",
        help="Use local storage paths instead of S3",
    )

    args, _ = parser.parse_known_args()
    return args


def main():
    """
    Main job execution pipeline.
    """
    args = parse_args()
    logger.info("Initializing AWS Glue / PySpark job...")
    spark = get_spark_session()

    source_path = args.source_path
    target_path = args.target_path

    # Support local fallback if S3 is not available
    if args.local_fallback:
        source_path = "/home/glue_user/workspace/data/raw/sample_input.csv"
        target_path = "/home/glue_user/workspace/data/processed/orders_processed"

    try:
        # 1. Extract
        raw_df = extract_data(spark, source_path)
        logger.info(f"Raw record count: {raw_df.count()}")
        raw_df.show(5, truncate=False)

        # 2. Transform
        clean_df = transform_data(raw_df)
        clean_count = clean_df.count()
        logger.info(f"Cleaned records count: {clean_count}")
        clean_df.show(5, truncate=False)

        # 3. Load
        load_data(clean_df, target_path, output_format=args.output_format)
        logger.info(f"ETL pipeline executed successfully! Processed {clean_count} records.")

    except Exception as e:
        logger.error(f"ETL Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

