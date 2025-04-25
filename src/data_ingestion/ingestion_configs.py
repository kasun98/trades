from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kraken
KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL")
SYMBOL = os.getenv("SYMBOL")

# kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TRADES_TOPIC = os.getenv("TRADES_TOPIC")
KAFKA_GROUP_PY = os.getenv("KAFKA_GROUP_PY")

# QuestDB
QDB_HOST = os.getenv("QDB_HOST")
QDB_PSYCOPG_PORT = os.getenv("QDB_PSYCOPG_PORT")
QDB_ILP_PORT = os.getenv("QDB_ILP_PORT")
QDB_USER = os.getenv("QDB_USER")
QDB_PASSWORD = os.getenv("QDB_PASSWORD")
QDB_DB = os.getenv("QDB_DB")
QDB_BRONZE_LAYER_TABLE=os.getenv("QDB_BRONZE_LAYER_TABLE")
QDB_SILVER_LAYER_TABLE=os.getenv("QDB_SILVER_LAYER_TABLE")
QDB_GOLD_LAYER_TABLE=os.getenv("QDB_GOLD_LAYER_TABLE")
QDB_EXPIRATION_DAYS_BRONZE=os.getenv("QDB_EXPIRATION_DAYS_BRONZE")
QDB_EXPIRATION_DAYS_SILVER=os.getenv("QDB_EXPIRATION_DAYS_SILVER")
QDB_EXPIRATION_DAYS_GOLD=os.getenv("QDB_EXPIRATION_DAYS_GOLD")
QDB_GOLD_VIEW_1MIN=os.getenv("QDB_GOLD_VIEW_1MIN")
QDB_GOLD_VIEW_5MIN=os.getenv("QDB_GOLD_VIEW_5MIN")
QDB_GOLD_VIEW_EXPIRATION_HOURS=os.getenv("QDB_GOLD_VIEW_EXPIRATION_HOURS")
QDB_GOLD_BID_PRED_TABLE=os.getenv("QDB_GOLD_BID_PRED_TABLE")
QDB_GOLD_VOL_PRED_TABLE=os.getenv("QDB_GOLD_VOL_PRED_TABLE")
QDB_BID_TASK_NAME = os.getenv("BID_TASK_NAME")
QDB_VOL_TASK_NAME = os.getenv("VOL_TASK_NAME")


# Redis
REDIS_HOST=os.getenv("REDIS_HOST")
REDIS_PORT=os.getenv("REDIS_PORT")
REDIS_BTC_SYMBOL=os.getenv("REDIS_BTC_SYMBOL")
REDIS_DB_INT=os.getenv("REDIS_DB_INT")
REDIS_TTL=os.getenv("REDIS_TTL")


# MINIO
MINIO_HOST=os.getenv("MINIO_HOST")
MINIO_USER_ID=os.getenv("MINIO_USER_ID")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
MINIO_MLFLOW_BUCKET=os.getenv("MINIO_MLFLOW_BUCKET")
MINIO_DATALAKE_BUCKET=os.getenv("MINIO_DATALAKE_BUCKET")


# Features
CH_COLUMNS = [
    "timestamp", "symbol", "bid", "bid_qty", "ask", "ask_qty", "last", "volume",
    "vwap", "low", "high", "change", "change_pct", "ma_5", "ma_14", "ema_5",
    "std_14", "price_change", "price_change_pct", "max_14", "min_14",
    "vwap_diff", "bid_ask_spread", "log_return", "momentum", "volatility",
    "cumulative_volume", "volume_change", "mean_bid_qty", "mean_ask_qty"
]

FEATURE_COLS = [
    'bid', 'bid_qty', 'ask', 'ask_qty', 'last', 'volume', 'vwap', 'low', 'high',
    'change', 'change_pct', 'ma_5', 'ma_14', 'ema_5', 'std_14', 'price_change',
    'price_change_pct', 'max_14', 'min_14', 'vwap_diff', 'bid_ask_spread',
    'log_return', 'momentum', 'volatility', 'cumulative_volume', 'volume_change',
    'mean_bid_qty', 'mean_ask_qty'
]

