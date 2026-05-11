import os
os.environ["PYSPARK_PYTHON"] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

JARS_DIR = os.path.expanduser("~/spark_jars")

JARS = ",".join([
    f"{JARS_DIR}/delta-core_2.12-2.4.0.jar",
    f"{JARS_DIR}/delta-storage-2.4.0.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{JARS_DIR}/antlr4-runtime-4.9.3.jar",
])

print(f"\nLoading JARs from: {JARS_DIR}")
for jar in JARS.split(","):
    exists = os.path.exists(jar)
    print(f"  {'[OK]' if exists else '[MISSING]'}  {jar}")

spark = SparkSession.builder \
    .appName("LocalLakehouseTest") \
    .master("local[*]") \
    .config("spark.jars", JARS) \
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
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n--- Writing test data to Bronze (MinIO) ---")

schema = StructType([
    StructField("event_id",   StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("ts",         StringType(), True),
])

data = [
    ("e001", "p999", "2024-01-01T00:00:00Z"),
    ("e002", "p001", "2024-01-01T00:00:01Z"),
    ("e003", "p999", "2024-01-01T00:00:02Z"),
]

df = spark.createDataFrame(data, schema)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://bronze/test_events/")

print("Write successful!")

print("\n--- Reading back from Bronze (MinIO) ---")
result = spark.read.format("delta").load("s3a://bronze/test_events/")
result.show()

print(f"Row count: {result.count()}")
print("\n[ALL CHECKS PASSED] Local mirror environment is fully operational.")

spark.stop()