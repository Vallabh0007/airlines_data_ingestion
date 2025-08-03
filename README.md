# Veridian Airlines Demo (Laptop-Only)

This repo simulates Veridian Airlines' multi-cloud analytics stack locally using Docker Compose.

## Services
- Azurite (Azure Blob emulator)
- LocalStack (AWS S3 emulator)
- Redpanda (Kafka/MSK emulator)
- Postgres (AWS RDS emulator)
- Data Generator (`generator.py`)

## Setup
```bash
docker compose up -d
pip install -r requirements.txt
python generator.py
