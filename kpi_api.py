# kpi_api.py
import uvicorn
import threading
import time
from fastapi import FastAPI, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as F_sum, current_timestamp
from datetime import timedelta

# -----------------------------------------
# SPARK SESSION
# -----------------------------------------

spark = (
    SparkSession.builder.appName("KPIService")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


GOLD_PATH = "./data/gold"
BRONZE_PATH="./data/bronze"
CACHE_TTL = 30  # refresh every 30 seconds

# Global cache
kpi_cache = {"data": {}, "last_updated": 0}


# -----------------------------------------
# KPI COMPUTATION
# -----------------------------------------
def compute_kpis():
    try:
        orders = spark.read.format("delta").load(f"{GOLD_PATH}/fact_orders")
        clickstream = spark.read.format("delta").load(f"{BRONZE_PATH}/clickstream")

        # KPI 1: Total ticket revenue in last 1 hour
        last_hour = orders.filter(
            col("timestamp") >= (current_timestamp() - timedelta(hours=1))
        )
        revenue = last_hour.agg(F_sum("amount")).collect()[0][0]

        # KPI 2: Active sessions in last 15 minutes
        active_sessions = clickstream.filter(
            col("timestamp") >= (current_timestamp() - timedelta(minutes=15))
        ).select("user").distinct().count()

        return {
            "total_ticket_revenue_last_hour": float(revenue or 0.0),
            "active_sessions_last_15_minutes": active_sessions,
        }
    except Exception as e:
        print(f"[KPI ERROR] {e}")
        return kpi_cache["data"]  # fallback to last cache


# Background thread to refresh cache
def refresh_cache():
    while True:
        kpis = compute_kpis()
        kpi_cache["data"] = kpis
        kpi_cache["last_updated"] = time.time()
        print(f"[CACHE] Refreshed at {time.ctime()} → {kpis}")
        time.sleep(CACHE_TTL)


# -----------------------------------------
# FASTAPI APP
# -----------------------------------------
app = FastAPI(title="KPI Service", version="1.0")

@app.on_event("startup")
def start_background_tasks():
    thread = threading.Thread(target=refresh_cache, daemon=True)
    thread.start()

@app.get("/kpi")
def get_kpi(name: str = Query(..., description="KPI name")):
    if name not in kpi_cache["data"]:
        return {"error": f"KPI '{name}' not found"}
    return {name: kpi_cache["data"][name]}


# -----------------------------------------
# MAIN
# -----------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
