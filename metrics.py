import os
import time
from pyspark.sql import functions as F

def write_prometheus_metrics(fact_orders, clickstream_df, metrics_dir="./metrics"):
    os.makedirs(metrics_dir, exist_ok=True)

    # KPI 1: Total ticket revenue in the last hour
    revenue = (
        fact_orders
        .filter(F.col("timestamp") >= F.expr("current_timestamp() - INTERVAL 1 HOUR"))
        .agg(F.sum("amount").alias("revenue"))
        .collect()[0]["revenue"]
    ) or 0

    # KPI 2: Active sessions in last 15 minutes
    sessions = (
        clickstream_df
        .filter(F.col("timestamp") >= F.expr("current_timestamp() - INTERVAL 15 MINUTES"))
        .select("user")
        .distinct()
        .count()
    )

    timestamp = int(time.time())
    metrics_path = os.path.join(metrics_dir, "kpis.prom")
    with open(metrics_path, "w") as f:
        f.write(f"# HELP ticket_revenue_last_hour Total ticket revenue in last hour\n")
        f.write(f"# TYPE ticket_revenue_last_hour gauge\n")
        f.write(f"ticket_revenue_last_hour {revenue} {timestamp}\n\n")

        f.write(f"# HELP active_sessions_last_15m Active sessions in last 15 minutes\n")
        f.write(f"# TYPE active_sessions_last_15m gauge\n")
        f.write(f"active_sessions_last_15m {sessions} {timestamp}\n")

    print(f"✅ Metrics written to {metrics_path}")
