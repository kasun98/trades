import json
from kafka import KafkaConsumer
from datetime import datetime
from collections import defaultdict, deque
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from ingestion_configs import (
    KAFKA_BROKER, TRADES_TOPIC, KAFKA_GROUP_PY,
    QDB_HOST, QDB_ILP_PORT, QDB_USER, QDB_PASSWORD, QDB_BRONZE_LAYER_TABLE,QDB_SILVER_LAYER_TABLE,
    REDIS_HOST, REDIS_PORT, REDIS_BTC_SYMBOL, REDIS_DB_INT, REDIS_TTL
)
from ingestion_utils import (
    send_ilp_to_questdb_ilp_host, compute_features,
    RedisFeatureStore
)

from logger.logger import setup_logger

logger = setup_logger("consumer")

WINDOW_SIZE = 30  # rolling window size
price_buffer = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


# Initialize Redis feature store
store = RedisFeatureStore(REDIS_HOST, REDIS_PORT, db=REDIS_DB_INT)


def create_kafka_consumer():
    logger.info("Creating Kafka consumer...")
    return KafkaConsumer(
        TRADES_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_PY
    )


def process_message(data):
    try:
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
            
        row = {
            "symbol":data['symbol'], 
            "bid":data['bid'], 
            "bid_qty": data['bid_qty'], 
            "ask": data['ask'], 
            "ask_qty":  data['ask_qty'],
            "last": data['last'], 
            "volume":  data['volume'], 
            "vwap": data['vwap'], 
            "low": data['low'], 
            "high": data['high'],
            "change": data['change'], 
            "change_pct": data['change_pct'], 
            "timestamp": data['timestamp']
        }

        # Bronze layer insert
        send_ilp_to_questdb_ilp_host( QDB_HOST, QDB_ILP_PORT, QDB_BRONZE_LAYER_TABLE, row)
        logger.info(f"Inserting tick for {data['symbol']} at {data['timestamp']}")
        logger.info(f"Updating Bronze layer for {data['symbol']}")

        
        features_to_write, features_for_model = compute_features(price_buffer, data['symbol'], data)
        
        # Store features in Redis
        if features_for_model:
            try:
                store.store(REDIS_BTC_SYMBOL, features_for_model)
            except Exception as redis_err:
                logger.warning(f"Redis store error for {data['symbol']}: {redis_err}")

        # Silver layer insert
        if features_to_write:
            send_ilp_to_questdb_ilp_host( QDB_HOST, QDB_ILP_PORT, QDB_SILVER_LAYER_TABLE, features_to_write)
            logger.info(f"Updating Silver layer for {data['symbol']}")

    except Exception as e:
        logger.error(f"Error processing data: {e}")


def run_consumer():
    consumer = create_kafka_consumer()
    logger.info(f"Listening to Kafka topic: {TRADES_TOPIC}")

    for message in consumer:
        data = message.value
        process_message(data)


if __name__ == "__main__":
    try:
        logger.info("QuestDB Kafka consumer starting...")
        run_consumer()
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
