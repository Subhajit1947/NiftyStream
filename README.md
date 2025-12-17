# 📈 NiftyStock Stream Analytics

A real-time stock market analytics platform that processes Nifty 50 stock data using Apache Spark Streaming, Kafka, and PostgreSQL.

**Core tech stack:**  
Python, Kafka, Apache Spark, PostgreSQL, Docker, Docker Compose, Streamlit (dashboard), Kafka-Python, Delta Lake.



## 🚀 Features

- **Real-time Data Processing**: Ingest and process 5-minute interval stock data from Kafka
- **Technical Indicators**: Calculate SMA, RSI, VWAP, volume surges, price spikes
- **Alert System**: Detect and store market alerts (price spikes, volume surges, RSI extremes)
- **Dashboard Ready**: Pre-processed data stored in PostgreSQL for real-time dashboards
- **Scalable Architecture**: Built with Spark Streaming for horizontal scaling



## Architecture

![NiftyStream Architecture](https://github.com/Subhajit1947/NiftyStream/blob/main/docs/stock_data_pipeline_architechture.png)


## Setup Guide (Local)

This guide describes how to run **NiftyStream** locally using Docker, Apache Spark, Kafka, and PostgreSQL.

---

### Prerequisites

- Docker  
- Docker Compose  
- Git  

---

### Local Setup

Clone the repository and move into the project directory:

```bash
git clone https://github.com/Subhajit1947/NiftyStream/
cd NiftyStream
```
### Create a .env file in the project root and configure the environment variables:
```bash
POSTGRES_HOST=""
POSTGRES_PORT=""
POSTGRES_DB=""
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_URL =""
ALERT_TABLE=""
STOCK_TABLE=""
STREAMLIT_SERVER_PORT=
STREAMLIT_SERVER_ADDRESS=
STREAMLIT_THEME=
STREAMLIT_BROWSER_GATHER_USAGE_STATS=
```
### Build Docker images and start all services:
```bash
docker-compose up -d --build
```
### check running containers
```bash
docker-compose ps
```
### fix spark ivy permissions
```bash
docker-compose exec --user root spark-master bash -c "
mkdir -p /nonexistent/.ivy2/cache &&
chmod 777 /nonexistent/.ivy2 &&
chmod 777 /nonexistent/.ivy2/cache
"
```
### run spark streaming job
```bash
docker-compose exec spark-master spark-submit \
  --packages \
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,\
  org.postgresql:postgresql:42.7.3 \
  /opt/apps/spark_streaming_test.py
```
### check spark logs
```bash
docker-compose logs spark-master
```


## 📊 Technical Indicators Calculated

| Indicator | Description | Alert Threshold |
|-----------|-------------|-----------------|
| **Price Change** | Current close vs open price | - |
| **SMA-5** | 5-period Simple Moving Average | - |
| **SMA-20** | 20-period Simple Moving Average | - |
| **Volume Ratio** | Current volume vs 20-period average | > 3x (Volume Surge) |
| **RSI** | 14-period Relative Strength Index | <30 (Oversold), >70 (Overbought) |
| **Price Spike** | 5-minute percentage change | > ±2% |
| **VWAP** | Volume Weighted Average Price | - |
