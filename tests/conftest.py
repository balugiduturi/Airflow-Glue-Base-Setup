"""
PyTest Configuration and Fixtures for AWS Glue PySpark Unit Tests.
"""

import sys
import os
import pytest
from pyspark.sql import SparkSession

# Ensure glue_jobs and project root are importable
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GLUE_JOBS_DIR = os.path.join(PROJECT_ROOT, "glue_jobs")

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if GLUE_JOBS_DIR not in sys.path:
    sys.path.insert(0, GLUE_JOBS_DIR)


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped local SparkSession configured for rapid, isolated unit testing.
    """
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("GluePySparkTestSession")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()

