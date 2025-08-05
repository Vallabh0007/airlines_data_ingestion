import os
import sys
import glob
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

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
# PATHS (read from CLI or fallback)
# -----------------------------------------
BASE_PATH = sys.argv[1] if len(sys.argv) > 1 else "./data"
STAGING_PATH = os.path.join(BASE_PATH, "staging")
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")

os.makedirs(BRONZE_PATH, exist_ok=True)
os.makedirs(os.path.join(STAGING_PATH, "bookings"), exist_ok=True)

# -----------------------------------------
# CLICKSTREAM INGEST
# -----------------------------------------
def ingest_clickstream():
    input_path = os.path.join(STAGING_PATH, "clickstream")
    output_path = os.path.join(BRONZE_PATH, "clickstream")

    if not os.path.exists(input_path) or not os.listdir(input_path):
        print(f"[Bronze] No clickstream files found at {input_path}, skipping.")
        return

    schema = StructType([
        StructField("user", StringType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    clickstream_df = spark.read.schema(schema).json(input_path)

    clickstream_df = (
        clickstream_df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("timestamp"))
        .dropDuplicates(["user", "timestamp", "page"])
    )

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

    print(f"[Bronze] Clickstream ingested to {output_path}")

# -----------------------------------------
# BOOKINGS INGEST
# -----------------------------------------
def ingest_bookings():
    input_path = os.path.expanduser(os.path.join(STAGING_PATH, "bookings", "*.csv"))
    output_path = os.path.join(BRONZE_PATH, "bookings")

    booking_files = glob.glob(input_path)
    if not booking_files:
        print(f"[Bronze] No booking files found at {input_path}, skipping.")
        return

    bookings_schema = StructType([
        StructField("booking_id", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    bookings_df = (
        spark.read.schema(bookings_schema)
        .option("header", True)
        .csv(booking_files)
    )

    bookings_df = (
        bookings_df
        .withColumn("booking_date", F.to_date("timestamp"))
        .dropDuplicates(["booking_id"])
    )

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

    print(f"[Bronze] Bookings ingested to {output_path}")

# -----------------------------------------
# MAIN
# -----------------------------------------
if __name__ == "__main__":
    ingest_clickstream()
    ingest_bookings()
    print("[Bronze] Ingestion complete")
    spark.stop()
