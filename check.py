from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# -----------------------------------------
# SPARK SESSION WITH DELTA PACKAGE
# -----------------------------------------
spark = (
    SparkSession.builder.appName("VerifyBronzeDelta")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

bronze_clickstream = "./data/bronze/clickstream"
bronze_bookings = "./data/bronze/bookings"

# -----------------------------------------
# HELPER FUNCTION
# -----------------------------------------
def verify_table(path, name, dedup_cols=None):
    if DeltaTable.isDeltaTable(spark, path):
        df = spark.read.format("delta").load(path)
        print(f"\n===== {name.upper()} =====")
        print(f"Total rows: {df.count()}")
        print("Sample data:")
        df.show(5, truncate=False)

        if "event_date" in df.columns:
            print("Partitions:", df.select("event_date").distinct().collect())

        if dedup_cols:
            dupes = (
                df.groupBy(dedup_cols)
                .count()
                .filter("count > 1")
            )
            if dupes.count() > 0:
                print(f"⚠️ Found duplicates in {name}:")
                dupes.show()
            else:
                print(f"✅ No duplicates in {name}.")
    else:
        print(f"⚠️ {name} is not a Delta table.")

# -----------------------------------------
# VERIFY TABLES
# -----------------------------------------
verify_table(bronze_clickstream, "clickstream", ["user", "timestamp", "page"])
verify_table(bronze_bookings, "bookings", ["booking_id"])
