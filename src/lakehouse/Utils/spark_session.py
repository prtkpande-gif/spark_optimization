# src/lakehouse/utils/spark_session.py
"""
Shared SparkSession factory for local and cloud environments.
All pipeline modules import get_spark() from here — never build
SparkSession directly in pipeline code.
"""

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "Lakehouse") -> SparkSession:
    """
    Returns a SparkSession configured for the current environment.
    Detects local vs cloud automatically via the ENV variable.

    Usage:
        from src.lakehouse.utils.spark_session import get_spark
        spark = get_spark("BronzeIngest")
    """
    env = os.getenv("LAKEHOUSE_ENV", "local")

    if env == "local":
        return _local_session(app_name)
    elif env == "cloud":
        return _cloud_session(app_name)
    else:
        raise ValueError(f"Unknown LAKEHOUSE_ENV: {env}. Use 'local' or 'cloud'.")


def _local_session(app_name: str) -> SparkSession:
    """
    Local session — uses Docker MinIO as S3, pre-downloaded JARs.
    Zero cloud cost.
    """
    jars_dir = os.path.expanduser("~/spark_jars")

    jars = ",".join([
        f"{jars_dir}/delta-core_2.12-2.4.0.jar",
        f"{jars_dir}/delta-storage-2.4.0.jar",
        f"{jars_dir}/hadoop-aws-3.3.4.jar",
        f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar",
        f"{jars_dir}/antlr4-runtime-4.9.3.jar",
    ])

    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", jars) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint",
                "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def _cloud_session(app_name: str) -> SparkSession:
    """
    Cloud session — Databricks manages JARs and S3 credentials via IAM.
    Config is minimal because Databricks Runtime handles the rest.
    Populated fully in Phase 6.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
