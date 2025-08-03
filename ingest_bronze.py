# ingest_bronze.py
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import glob

# -----------------------------------------
# SPARK SESSION
# -----------------------------------------
builder = (
    SparkSession.builder.appName("DeltaLakeIngest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# -----------------------------------------
# PATHS
# -----------------------------------------
BRONZE_PATH = "./data/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)


# -----------------------------------------
# CLICKSTREAM INGEST
# -----------------------------------------
def ingest_clickstream():
    input_path = "./data/staging/clickstream"
    output_path = f"{BRONZE_PATH}/clickstream"

    # Explicit schema to avoid infer error
    schema = StructType([
        StructField("user", StringType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    clickstream_df = spark.read.schema(schema).json(input_path)

    # Convert timestamp to proper type
    clickstream_df = clickstream_df.withColumn(
        "timestamp", F.to_timestamp("timestamp")
    )

    # Add event_date column for partitioning
    clickstream_df = clickstream_df.withColumn("event_date", F.to_date("timestamp"))

    # Deduplicate on (user, timestamp, page)
    clickstream_df = clickstream_df.dropDuplicates(["user", "timestamp", "page"])

    if DeltaTable.isDeltaTable(spark, output_path):
        bronze_table = DeltaTable.forPath(spark, output_path)
        (
            bronze_table.alias("t")
            .merge(
                clickstream_df.alias("s"),
                "t.user = s.user AND t.timestamp = s.timestamp AND t.page = s.page",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            clickstream_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(output_path)
        )

    print("[Bronze] Clickstream ingested with deduplication & late-arrival handling")

# -----------------------------------------
# BOOKINGS INGEST
# -----------------------------------------
os.makedirs("./data/staging/bookings", exist_ok=True)

def ingest_bookings():
    input_path = "./data/staging/bookings/*.csv"
    output_path = f"{BRONZE_PATH}/bookings"
    if not glob.glob(input_path):
        print("[Bronze] No booking files found, skipping ingestion.")
        return
    # Define schema explicitly
    bookings_schema = StructType([
        StructField("booking_id", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    bookings_df = (
        spark.read.schema(bookings_schema)
        .option("header", True)
        .csv(input_path)
    )

    # Add booking_date column for partitioning
    bookings_df = bookings_df.withColumn("booking_date", F.to_date("timestamp"))

    # Deduplicate on booking_id
    bookings_df = bookings_df.dropDuplicates(["booking_id"])

    if DeltaTable.isDeltaTable(spark, output_path):
        bronze_table = DeltaTable.forPath(spark, output_path)
        (
            bronze_table.alias("t")
            .merge(
                bookings_df.alias("s"),
                "t.booking_id = s.booking_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            bookings_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("booking_date")
            .save(output_path)
        )

    print("[Bronze] Bookings ingested with deduplication & late-arrival handling")

# -----------------------------------------
# MAIN
# -----------------------------------------
if __name__ == "__main__":
    ingest_clickstream()
    ingest_bookings()
    print("[Bronze] Ingestion complete")
