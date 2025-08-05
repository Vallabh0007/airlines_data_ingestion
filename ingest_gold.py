# ingest_gold.py
import os
import sys
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

# -----------------------------------------
# SPARK SESSION
# -----------------------------------------
spark = (
    SparkSession.builder.appName("DeltaLakeTransform")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .getOrCreate()
)

# -----------------------------------------
# PATH SETUP
# -----------------------------------------
DEFAULT_BASE_PATH = os.path.expanduser("~/vallabh/assign/data")
BASE_PATH = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BASE_PATH
BASE_PATH = os.path.abspath(BASE_PATH)

# Strip Airflow's /tmp prefix if injected
if BASE_PATH.startswith("/tmp/airflowtmp"):
    BASE_PATH = DEFAULT_BASE_PATH

BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
GOLD_PATH = os.path.join(BASE_PATH, "gold")

print(f"📂 Using BASE_PATH: {BASE_PATH}")
print(f"📂 Bronze path: {BRONZE_PATH}")
print(f"📂 Gold path: {GOLD_PATH}")

os.makedirs(GOLD_PATH, exist_ok=True)

# -----------------------------------------
# SAFE DELTA READER
# -----------------------------------------
def safe_read_delta(path):
    if not os.path.exists(path):
        print(f"❌ Path not found: {path}")
        sys.exit(1)
    return spark.read.format("delta").load(path)

# -----------------------------------------
# DIMENSIONS
# -----------------------------------------
def build_dim_time(bookings_df):
    dim_time = (
        bookings_df
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("minute", F.minute("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .select("timestamp", "date", "hour", "minute", "day", "month", "year")
        .dropDuplicates(["timestamp"])
        .withColumn("time_id", F.monotonically_increasing_id())
    )
    dim_time.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_time")
    return dim_time

def build_dim_customer(bookings_df):
    dim_customer = (
        bookings_df.select("customer")
        .dropDuplicates()
        .withColumn("customer_id", F.monotonically_increasing_id())
    )
    dim_customer.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_customer")
    return dim_customer

def build_dim_product(clickstream_df):
    dim_product = (
        clickstream_df.select("page")
        .dropDuplicates()
        .withColumn("product_id", F.monotonically_increasing_id())
    )
    dim_product.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_product")
    return dim_product

# -----------------------------------------
# FACT TABLE
# -----------------------------------------
def build_fact_orders(bookings_df, dim_time, dim_customer):
    fact_orders = (
        bookings_df
        .join(dim_time, "timestamp")
        .join(dim_customer, "customer")
        .select(
            "booking_id",
            "customer_id",
            "time_id",
            "amount",
            "timestamp"
        )
    )
    fact_orders.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/fact_orders")
    return fact_orders

# -----------------------------------------
# KPIs
# -----------------------------------------
def compute_kpis(fact_orders, clickstream_df):
    # KPI 1: Total ticket revenue in the last hour
    revenue_last_hour = (
        fact_orders
        .filter(F.col("timestamp") >= F.expr("current_timestamp() - INTERVAL 1 HOUR"))
        .agg(F.sum("amount").alias("total_ticket_revenue_last_hour"))
    )

    # KPI 2: Active sessions in the last 15 minutes
    active_sessions = (
        clickstream_df
        .filter(F.col("timestamp") >= F.expr("current_timestamp() - INTERVAL 15 MINUTES"))
        .select("user")
        .distinct()
        .agg(F.count("user").alias("active_sessions_last_15_minutes"))
    )

    kpis = revenue_last_hour.crossJoin(active_sessions)
    kpis.show(truncate=False)

# -----------------------------------------
# MAIN
# -----------------------------------------
if __name__ == "__main__":
    bookings_df = safe_read_delta(f"{BRONZE_PATH}/bookings")
    clickstream_df = safe_read_delta(f"{BRONZE_PATH}/clickstream")

    # Build dimensions
    dim_time = build_dim_time(bookings_df)
    dim_customer = build_dim_customer(bookings_df)
    dim_product = build_dim_product(clickstream_df)

    # Build fact_orders
    fact_orders = build_fact_orders(bookings_df, dim_time, dim_customer)

    # Compute KPIs
    compute_kpis(fact_orders, clickstream_df)

    print("✅ Step 4 complete: Star schema + KPIs created.")
