# ⚡ Realtime Data Streaming Platform

### 🚀 Production-Grade Data Engineering Pipeline (Kafka + Spark + Airflow)

<p align="center">
  <img src="https://img.shields.io/badge/Kafka-Streaming-black?logo=apachekafka"/>
  <img src="https://img.shields.io/badge/Spark-Processing-orange?logo=apachespark"/>
  <img src="https://img.shields.io/badge/Airflow-Orchestration-blue?logo=apacheairflow"/>
  <img src="https://img.shields.io/badge/Cassandra-Storage-green?logo=apachecassandra"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker"/>
</p>

---

# 🧠 Overview

A real-time distributed data engineering pipeline that ingests, streams, processes, and stores user event data using modern big data technologies.

```text
RandomUser API
        ↓
Apache Airflow
        ↓
Apache Kafka
        ↓
Spark Structured Streaming
        ↓
Apache Cassandra
```

### ✅ Key Features

- Real-time event streaming pipeline
- Distributed stream processing
- Fault-tolerant architecture
- Dockerized infrastructure
- Scalable storage layer
- End-to-end orchestration

---

# 🎬 Demo

> 📌 Add a GIF or short demo video here showing:
>
> - Airflow DAG running
> - Kafka topic streaming
> - Spark processing
> - Cassandra data insertion
> - Docker containers

This significantly improves recruiter engagement.

---

# 🏗️ Architecture
<img width="3274" height="1221" alt="Data engineering architecture" src="https://github.com/user-attachments/assets/db4277e9-a65a-498c-bae4-5386a036faf0" />


---

# ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Data Source | RandomUser API |
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka + Zookeeper |
| Stream Processing | Spark Structured Streaming |
| Storage | Apache Cassandra |
| Metadata DB | PostgreSQL |
| Monitoring | Control Center + Schema Registry |
| Infrastructure | Docker + Docker Compose |

---

# 🔄 Pipeline Workflow

## 1️⃣ Data Ingestion

- Airflow DAG fetches synthetic user data from RandomUser API
- Data is formatted into structured JSON events

## 2️⃣ Kafka Streaming

- Events are published to Kafka topic:
  
```text
users_created
```

- Kafka acts as a distributed event broker

## 3️⃣ Stream Processing

- Spark Structured Streaming consumes Kafka events
- JSON events are parsed and transformed
- Streaming DataFrame is generated in real time

## 4️⃣ Data Storage

- Processed streaming records are written into Cassandra
- Cassandra provides scalable distributed storage

---

# 📁 Project Structure

```bash
realtime-data-streaming-platform/
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
├── README.md
│
├── airflow/
│   ├── dags/
│   │   └── airflow_kafka_producer.py
│   └── entrypoint.sh
│
├── spark/
│   └── spark_streaming_consumer.py
│
├── assets/
│   └── architecture.png
│
└── tests/
```

---

# 🚀 Quick Start

## 1️⃣ Clone Repository

```bash
git clone https://github.com/sujoy-halder/Realtime-Data-Streaming-Pipeline.git

cd realtime-data-streaming-platform
```

## 2️⃣ Start Containers

```bash
docker-compose up -d
```

## 3️⃣ Verify Running Containers

```bash
docker ps
```

---

# 📡 Create Kafka Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic users_created \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

---

# ▶️ Run Spark Streaming Consumer

```bash
python spark/spark_streaming_consumer.py
```

---

# 📊 Observability

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Spark Master UI | http://localhost:9090 |
| Kafka Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |

---

# 🧪 Testing

```bash
pytest tests/
```

---

# 💡 Engineering Highlights

- Real-time streaming architecture
- Distributed systems design
- Event-driven data pipeline
- Structured stream processing
- Containerized deployment
- Scalable NoSQL storage
- Fault-tolerant pipeline architecture

---

# 🏆 Resume Value

### Built a production-style real-time data platform using:

- Apache Kafka
- Spark Structured Streaming
- Apache Airflow
- Cassandra
- Docker

### Key Achievements

- Designed distributed event-driven architecture
- Implemented scalable streaming ingestion pipeline
- Built fault-tolerant processing system
- Automated orchestration using Airflow
- Containerized full infrastructure using Docker Compose

---

# 📌 Future Improvements

- Add Prometheus + Grafana monitoring
- Implement Avro serialization
- Add Schema Evolution support
- Deploy on AWS / GCP
- Add CI/CD pipeline using GitHub Actions
- Integrate dbt for analytics transformation

---

# 👨‍💻 Author

## Sujoy Halder

- Data Engineering
- Big Data & Streaming Systems
- Kafka + Spark + Airflow

---

# ⭐ Support

If you found this project useful, give it a ⭐ on GitHub.
