from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor
from airflow.operators.python import PythonOperator

# Path to your scripts
BASE_PATH = "/home/vallabh/vallabh/assign"
INGEST_BRONZE = f"{BASE_PATH}/ingest_bronze.py"
INGEST_GOLD = f"{BASE_PATH}/ingest_gold.py"

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def publish_kpi():
    import subprocess
    subprocess.run(["python", INGEST_GOLD], check=True)

with DAG(
    dag_id="etl_kpi_dag",
    default_args=default_args,
    description="ETL pipeline with Kafka + Scheduled runs",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2025, 8, 5),
    catchup=False,
    tags=["etl", "delta", "kpi"],
) as dag:

    # ✅ Kafka Sensor to wait for new batch (replace topic details as needed)
    kafka_wait = KafkaSensor(
        task_id="wait_for_kafka",
        topics=["clickstream"],  # your Kafka topic
        kafka_config_id="kafka_default",  # set in Airflow Connections
        poll_timeout=5,
        timeout=600,  # 10 mins max wait
    )

    load = BashOperator(
        task_id="load_bronze",
        bash_command=f"python {INGEST_BRONZE}"
    )

    transform = BashOperator(
        task_id="transform_gold",
        bash_command=f"python {INGEST_GOLD}"
    )

    publish = PythonOperator(
        task_id="publish_kpi",
        python_callable=publish_kpi,
    )

    # DAG flow: Kafka triggers or schedule → load → transform → publish
    kafka_wait >> load >> transform >> publish
    dag.doc_md = __doc__
