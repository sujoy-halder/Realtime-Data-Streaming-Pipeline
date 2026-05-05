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

## 🧠 What This Project Does

A **real-time, distributed data pipeline** that ingests, streams, processes, and stores data at scale.

```text
API → Airflow → PostgreSQL → Kafka → Spark → Cassandra
```

✔ Simulates real-world streaming systems (like Netflix / Uber)
✔ Handles real-time ingestion + processing
✔ Built with scalable, fault-tolerant components

---

## 🎬 Demo (Add GIF here)

> 🔴 *Tip: Record your screen (Kafka + Spark + logs running) and upload a GIF here*
> This is what makes recruiters pause.

---

## 🏗️ Architecture

![Architecture](./assets/architecture.png)

---

## ⚙️ Tech Stack

| Layer          | Tool                            |
| -------------- | ------------------------------- |
| Ingestion      | randomuser API                  |
| Orchestration  | Apache Airflow                  |
| Streaming      | Apache Kafka + Zookeeper        |
| Processing     | Apache Spark                    |
| Storage        | PostgreSQL, Cassandra           |
| Monitoring     | Control Center, Schema Registry |
| Infrastructure | Docker                          |

---

## 🔄 How It Works

1. **Data Generation**

   * API generates synthetic user data

2. **Orchestration (Airflow)**

   * Schedules and loads data into PostgreSQL

3. **Streaming (Kafka)**

   * Streams events in real time

4. **Processing (Spark)**

   * Cleans + transforms streaming data

5. **Storage (Cassandra)**

   * Stores processed data for fast access

---

## 📁 Project Structure

```bash
realtime-data-platform/
│
├── docker-compose.yml
├── .env
│
├── api/                # Kafka Producer
├── airflow/            # DAGs
├── spark/              # Streaming jobs
├── cassandra/          # DB schema
├── kafka/              # Configs
├── scripts/            # Start/stop scripts
├── assets/             # Diagrams / GIFs
└── tests/              # Pipeline tests
```

---

## 🚀 Quick Start (1 Command Run)

```bash
make up
```

Or manually:

```bash
docker-compose up -d
```

---

## 📡 Create Kafka Topic

```bash
docker exec -it kafka kafka-topics \
--create \
--topic user-events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

---

## ▶️ Run Producer

```bash
python api/producer.py
```

---

## 📊 Observability (Very Important)

| Tool       | URL                   |
| ---------- | --------------------- |
| Kafka UI   | http://localhost:9021 |
| Spark UI   | http://localhost:8080 |
| Airflow UI | http://localhost:8080 |

---

## 🧪 Testing

```bash
pytest tests/
```

---

## 💡 Key Engineering Highlights

✔ Real-time streaming pipeline
✔ Distributed system design
✔ Fault-tolerant architecture
✔ Microservices-based setup
✔ Containerized infrastructure

---

## 🏆 Resume Value

* Built an **end-to-end real-time data pipeline** using Kafka, Spark, and Airflow
* Designed a **scalable distributed system handling streaming data**
* Implemented **event-driven architecture with containerized deployment**

---

## 📌 Future Improvements

* Add **Prometheus + Grafana monitoring**
* Deploy on **AWS / GCP**
* Add **CI/CD pipeline (GitHub Actions)**
* Implement **schema evolution (Avro)**

---

## 👨‍💻 Author

**Sujoy Halder**

---

## ⭐ If you found this useful

Give it a ⭐ and share — it helps a lot!
