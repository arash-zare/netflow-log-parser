# NetFlow → Kafka → ClickHouse → Grafana Pipeline

This project provides a full pipeline for collecting, processing, and visualizing NetFlow data.  
It uses **[goflow2](https://github.com/netsampler/goflow2)** to capture NetFlow/IPFIX/sFlow data,  
streams it into **Kafka**, stores it in **ClickHouse**, and visualizes it with **Grafana**.

---

## 📂 Project Structure




---

## 🔄 Data Flow

1. **Goflow2**  
   - Listens on UDP (e.g., `:2055`) for NetFlow/IPFIX/sFlow.  
   - Encodes records as protobuf (`flow.proto`).  
   - Produces them into **Kafka**.

2. **Kafka**  
   - Buffers NetFlow events.  
   - Provides decoupling between ingestion and storage.  

3. **ClickHouse**  
   - Consumes messages from Kafka.  
   - Tables are defined in `clickhouse/init.sql`.  
   - Stores flow records (bytes, packets, src/dst, ports, protocols, etc.).

4. **Grafana**  
   - Queries ClickHouse via the **ClickHouse datasource plugin**.  
   - Prebuilt dashboards available under `grafana/dashboards/`.

5. **Prometheus**  
   - Scrapes metrics from services (Kafka, ClickHouse, Goflow, etc.).  
   - Can be visualized in Grafana as well.

---

## 🚀 Getting Started

### 1. Clone Repository
```bash
git clone https://github.com/your-org/netflow-pipeline.git
cd netflow-pipeline

