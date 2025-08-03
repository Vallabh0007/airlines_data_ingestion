import os
import csv
import json
import time
import random
import boto3
import psycopg2
from azure.storage.blob import BlobServiceClient
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NodeNotReadyError, KafkaError
from datetime import datetime

# -----------------------------------------
# CONFIG
# -----------------------------------------
AZURE_CONN_STR = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
S3_ENDPOINT = "http://localstack:4566"
S3_BUCKET = "partner-bookings"
POSTGRES_CONN = "dbname=bookings user=booking_user password=booking_pass host=postgres port=5432"
KAFKA_BROKER = "redpanda:9092"
KAFKA_TOPIC = "clickstream"


# -----------------------------------------
# HELPERS
# -----------------------------------------
def generate_booking():
    return {
        "booking_id": random.randint(1000, 9999),
        "customer": random.choice(["Alice", "Bob", "Charlie", "Diana"]),
        "amount": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def generate_click_event():
    return {
        "user": random.choice(["Alice", "Bob", "Charlie", "Diana"]),
        "action": random.choice(["search", "view", "click", "purchase"]),
        "page": random.choice(["home", "flights", "checkout"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def ensure_kafka_topic(topic, broker):
    """Ensure Kafka topic exists, create if not."""
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=[broker], request_timeout_ms=10000)
            existing_topics = admin.list_topics()
            if topic not in existing_topics:
                print(f"[Kafka] Topic '{topic}' not found, creating...")
                new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
                admin.create_topics([new_topic])
                print(f"[Kafka] Topic '{topic}' created.")
            else:
                print(f"[Kafka] Topic '{topic}' already exists.")
            admin.close()
            break
        except NodeNotReadyError:
            print("[Kafka] Broker/controller not ready, retrying in 5s...")
            time.sleep(5)
        except KafkaError as e:
            print(f"[Kafka] Error: {e}, retrying in 5s...")
            time.sleep(5)

# -----------------------------------------
# MAIN LOOP
# -----------------------------------------
def main():
    # Ensure Kafka topic
    ensure_kafka_topic(KAFKA_TOPIC, KAFKA_BROKER)

    # Azure Blob client
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container_name = "analytics"
    try:
        blob_service_client.create_container(container_name)
    except Exception:
        pass  # container exists

    # LocalStack S3 client
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                      aws_access_key_id="test", aws_secret_access_key="test")
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass  # bucket exists

    # Kafka producer
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Postgres connection
    conn = psycopg2.connect(POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id INT PRIMARY KEY,
            customer TEXT,
            amount NUMERIC,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()

    hour_counter = 0
    while True:
        # 1️⃣ Clickstream: ~50 events every second
        for _ in range(50):
            event = generate_click_event()
            producer.send(KAFKA_TOPIC, event)
        print("[Kafka] Published 50 events")

        # 2️⃣ Hourly tasks (simulated every 60 cycles)
        if hour_counter >= 60:
            bookings = [generate_booking() for _ in range(10)]

            # 2a. Write CSV to Azurite
            csv_filename = f"bookings_{int(time.time())}.csv"
            with open(csv_filename, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=bookings[0].keys())
                writer.writeheader()
                writer.writerows(bookings)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_filename)
            with open(csv_filename, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"[Azurite] Uploaded {csv_filename}")

            # 2b. Write CSV to LocalStack S3
            s3.upload_file(csv_filename, S3_BUCKET, csv_filename)
            print(f"[LocalStack] Uploaded {csv_filename}")

            # 2c. Insert into Postgres
            for booking in bookings:
                cur.execute("""
                    INSERT INTO bookings (booking_id, customer, amount, timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (booking["booking_id"], booking["customer"], booking["amount"], booking["timestamp"]))
            conn.commit()
            print(f"[Postgres] Inserted {len(bookings)} rows")

            os.remove(csv_filename)  # cleanup temp file
            hour_counter = 0

        hour_counter += 1
        time.sleep(1)  # 1-second cycle

if __name__ == "__main__":
    main()
