# Real-Time BTC/USD Trades Data & ML Pipeline

This is a fully open-source, end-to-end real-time data pipeline project built around BTC/USD live trade data streamed via Kraken WebSocket. It covers everything from data ingestion, processing, and feature engineering, to real-time ML predictions, observability, and scheduled retraining, all in a production-grade architecture.

---

## 🧠 Architecture Overview

![Architecture Diagram](images/arch_diag.png)

This pipeline follows a Medallion Architecture on **QuestDB**:
- **Bronze**: Raw WebSocket trade data
- **Silver**: Engineered features (moving average, volatility, etc.)
- **Gold**: Materialized views & ML predictions (bid price & volatility)

---

## ⚙️ Tech Stack

- **WebSocket** (Kraken BTC/USD live trades)
- **Kafka (Confluent)** for streaming
- **Kafka Consumer** for real-time feature engineering
- **QuestDB** for time-series storage with Medallion architecture
- **MinIO** (Object Storage)
- **Redis** as real-time feature store (TTL + pub/sub)
- **XGBoost** for prediction models (volatility & bid price)
- **Flask** + **Waitress** for ML model serving
- **MLflow** + **MinIO** + **PostgreSQL** for experiment tracking
- **Grafana** for dashboarding
- **Airflow** for orchestration & scheduling
- **Docker Compose** for containerization

---

## 🔧 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/kasun98/trades.git
cd trades
```

### 2. Activate Virtual Environment
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Set Up Environment Variables
Rename the `rename.env` file to `.env`
Update credentials and config paths in `.env` file before proceeding:
```env
# Example
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_password
...
```

### 4. Start Docker Services (this may take a few minutes)
```bash
docker-compose up -d 
```

### 5. Add execution permission
```bash
chmod +x *.sh
```

### 6. Initialize Databases (one time)
```bash
./db.sh  # Linux/Mac
bash db.sh  # Windows (PowerShell)

# Or run this
python src/data_ingestion/init_questdb.py 
python src/data_ingestion/init_minio.py
```

### 7. Initialize MLflow server
```bash
./mlflow.sh  # Linux/Mac
bash mlflow.sh  # Windows (PowerShell)

# Or run this
$env:MLFLOW_S3_ENDPOINT_URL = "http://localhost:9002"
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
$env:AWS_DEFAULT_REGION = "us-east-1"
mlflow server --backend-store-uri postgresql://mlflow_user:mlflow_pass@localhost:5431/mlflow_db --artifacts-destination s3://mlflow/ --host 0.0.0.0 --port 5000
```

### 8. Initialize Model serving (new terminal)
```bash
./mmodels.sh  # Linux/Mac
bash models.sh  # Windows (PowerShell)

# Or run this
cd src/forecast 
waitress-serve --threads=4 --host=0.0.0.0 --port=5050 app:app
```

### 9. Star Ingestion (new terminal)
```bash
python src/data_ingestion/producer.py
```

### 10. Star Consumer (new terminal)
```bash
python src/data_ingestion/consumer.py
```

### 11. Star Downstream (new terminal)
```bash
python src/downstream/predictor.py
```

---

## 🧩 Components & Features

### Kafka Streaming (Confluent)
Streams raw BTC/USD WebSocket data to Kafka.
![Confluent UI](images/confluent_cc.png)

### Real-Time Feature Engineering
Kafka consumer generates features like:
- Moving Average
- Volatility
- Trade Volume
- ....

Features are:
- Stored in **Silver layer** of QuestDB
- Sent to **Redis** feature store with TTL (300s) and pub/sub

### ML Modeling with XGBoost
Two XGBoost models:
- **5-min Volatility Forecast**
- **5-min Bid Price Forecast**

MLflow tracks experiments:
![MLflow UI](images/mlflow_ui.png)

Model artifacts stored in **MinIO**:
![MinIO UI](images/minio_ui.png)

### Model Serving
Served using **Flask + Waitress**
- `/predict_bid` — Predict bid price using latest features
- `/predict_vol` — Predict volatility using latest features
- `/retrain` — Retrain ML models
- `/reload` — Reload new model into memory
- `/test` — test GET
- `/test_post` — test POST


### Redis Feature Store
![Redis Commander UI](images/rediscommander_ui.png)
- Real-time feature vector store
- TTL-based cache
- Pub/Sub architecture for low-latency prediction

### QuestDB with Medallion Architecture
![QuestDB UI](images/qdb_ui.png)
- Bronze: Raw Data
- Silver: Engineered Features
- Gold: Predictions & Materialized Views

TTL of 2 days to manage storage footprint.

### Grafana Dashboards
![Grafana Dashboard](images/grafana_dashboard.png)
- Live features
- Predicted bid price & volatility with respect to current values
- Pipeline latency (end-to-end)

### Airflow Orchestration
- Daily retraining of models
- Daily backup of all QuestDB tables to MinIO data lake

---

## Monitoring & Visualization

| Tool        | URL                        | Description              |
|-------------|----------------------------|--------------------------|
| MLflow UI   | `http://localhost:5000`    | Experiment Tracking      |
| Airflow     | `http://localhost:8080`    | DAG Monitoring           |
| QuestDB     | `http://localhost:9000`    | QuestDB                  |
| Confluent   | `http://localhost:9021`    | Kafka Monitoring         |
| Grafana     | `http://localhost:3000`    | Dashboards               |
| MinIO       | `http://localhost:9001`    | Object Storage           |
| Redis-Com   | `http://localhost:8081`    | Redis UI                 |

---

## 🧹 Shut Down Services
To stop all containers:
```bash
docker-compose down
```

---

## License

- MIT License – see LICENSE for details.

---

## Acknowledgments

- Kraken Cryptocurrency Exchange
- Apache Airflow & Kafka
- Confluent Platform
- MLflow, MinIO, Redis
- Flask, Postgres, QuestDB

---

## Author

**Kasun Dewaka**  
Feel free to reach out via [LinkedIn](https://linkedin.com/in/kasundewaka/) or open an issue!

