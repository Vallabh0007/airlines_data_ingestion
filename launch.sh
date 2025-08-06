#!/bin/bash
set -e

echo "🚀 Starting ETL + Metrics Pipeline"

# Start Airflow
echo "📂 Initializing Airflow DB..."
airflow db migrate

echo "👷 Starting Airflow webserver (background)..."
airflow webserver -p 8080 > airflow_webserver.log 2>&1 &

echo "👷 Starting Airflow scheduler (background)..."
airflow scheduler > airflow_scheduler.log 2>&1 &

sleep 5

# Ensure DAGs are parsed
echo "✅ DAGs registered:"
airflow dags list

# Optionally start Kafka + generator
echo "📡 Starting Kafka generator..."
python ~/vallabh/assign/generator.py &

# Trigger DAG manually once
echo "▶️ Triggering DAG etl_kpi_dag..."
airflow dags trigger etl_kpi_dag

echo "✅ All systems launched! Check http://localhost:8080 for Airflow UI."
