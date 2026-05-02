from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

# --- CONFIGURATION LAYER ---
# Principle: Environment-agnostic code [cite: 232, 301]
RUN_ENV = "LOCAL" # Options: "LOCAL" or "CLOUD"

config = {
    "LOCAL": {
        "scale": 10000,
        "partitions": 2,
        "path": "./data/bronze"
    },
    "CLOUD": {
        "scale": 100000000,
        "partitions": 200,
        "path": "/mnt/datalake/bronze"
    }
}

current_config = config[RUN_ENV]

# Initialize Spark with Mechanical Sympathy [cite: 32, 252]
spark = SparkSession.builder \
    .appName(f"PrincipalDataFactory_{RUN_ENV}") \
    .config("spark.sql.shuffle.partitions", current_config["partitions"]) \
    .getOrCreate()

# --- SOURCE 1: CLICKSTREAM (With Intentional Skew) ---
# Principle: Inject "Controlled Chaos" to test system resilience [cite: 111, 171]
clickstream_df = spark.range(0, current_config["scale"], numPartitions=current_config["partitions"]) \
    .withColumn("user_id", (F.rand() * 100000).cast("int")) \
    .withColumn("product_id", 
                F.when(F.rand() < 0.1, 999) # THE SKEW: 10% of traffic hits one product [cite: 113, 164]
                .otherwise((F.rand() * 5000).cast("int"))) \
    .withColumn("event_time", F.current_timestamp() - F.expr("CAST(rand() * 3600 AS INTERVAL SECOND)")) \
    .withColumn("event_type", F.element_at(F.array(F.lit("click"), F.lit("view"), F.lit("purchase")), (F.rand() * 3 + 1).cast("int")))

# --- SOURCE 2: TRANSACTIONAL ORDERS (Dimension Data) ---
orders_df = spark.range(0, current_config["scale"] // 10) \
    .withColumn("order_id", F.col("id")) \
    .withColumn("product_id", (F.rand() * 5000).cast("int")) \
    .withColumn("price", (F.rand() * 100).cast("decimal(10,2)"))

# --- SOURCE 3: PRODUCT CATALOG (Reference Data) ---
products_data = [(i, f"Product_{i}", "Electronics" if i % 2 == 0 else "Apparel") for i in range(5001)]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category"])

# --- THE PRINCIPAL'S AUDIT COMMANDS ---

# 1. Execution Plan Audit
# This will print the Physical Plan to your console
clickstream_df.explain(True) 

# 2. Schema Verification
# Ensures data types are correct (e.g., product_id is an Integer)
clickstream_df.printSchema() 

# 3. Data Preview (The Action that triggers generation)
# This will materialize a small sample in your console
clickstream_df.show(5)

# --- OPTIMIZED WRITE (FinOps & Performance) ---
# Principle: Manage partitioning to avoid the "Small File Problem" [cite: 71, 167, 278]
clickstream_df.write.mode("overwrite").parquet(f"{current_config['path']}/clicks")
orders_df.write.mode("overwrite").parquet(f"{current_config['path']}/orders")
products_df.write.mode("overwrite").parquet(f"{current_config['path']}/products")

print(f"Platform Strategy Executed: {RUN_ENV} scale reached.")