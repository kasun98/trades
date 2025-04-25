import time
import threading
import redis
import copy
from concurrent.futures import ThreadPoolExecutor
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from utils import RedisFeatureFetcher, send_prediction_request
from downstream_configs import (REDIS_HOST, REDIS_PORT, REDIS_BTC_SYMBOL, REDIS_DB_INT, BID_MODEL_PREDICT_ENDPOINT, 
                                VOLATILITY_MODEL_PREDICT_ENDPOINT, QDB_HOST, QDB_ILP_PORT, QDB_GOLD_BID_PRED_TABLE, QDB_GOLD_VOL_PRED_TABLE,
                                QDB_BID_TASK_NAME, QDB_VOL_TASK_NAME)

from logger.logger import setup_logger
from data_ingestion.ingestion_configs import FEATURE_COLS

executor = ThreadPoolExecutor(max_workers=2)

logger = setup_logger("redis-predictor")

def subscribe_to_feature_updates():
    fetcher = RedisFeatureFetcher(REDIS_HOST, REDIS_PORT, db=REDIS_DB_INT)
    pubsub = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_INT).pubsub()
    pubsub.subscribe(f"feature_update:{REDIS_BTC_SYMBOL}")

    logger.info(f"Subscribed to feature updates on channel: feature_update:{REDIS_BTC_SYMBOL}")

    for message in pubsub.listen():
        if message["type"] == "message":
            logger.info("New feature vector message received")
            features = fetcher.fetch(REDIS_BTC_SYMBOL)

            if features:
                feature_vector = [features[col] for col in FEATURE_COLS]

                # Deep copy features to prevent overlap between tasks
                features_for_bid = copy.deepcopy(features)
                features_for_vol = copy.deepcopy(features)

                executor.submit(
                    send_prediction_request, BID_MODEL_PREDICT_ENDPOINT, feature_vector,
                    features_for_bid, QDB_HOST, QDB_ILP_PORT, QDB_GOLD_BID_PRED_TABLE, QDB_BID_TASK_NAME
                )

                executor.submit(
                    send_prediction_request, VOLATILITY_MODEL_PREDICT_ENDPOINT, feature_vector,
                    features_for_vol, QDB_HOST, QDB_ILP_PORT, QDB_GOLD_VOL_PRED_TABLE, QDB_VOL_TASK_NAME
                )

            else:
                logger.warning("No feature vector available.")

if __name__ == "__main__":
    try:
        logger.info("Starting Redis feature vector subscriber...")
        subscribe_to_feature_updates()
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user.")
    except Exception as e:
        logger.error(f"Error in subscriber: {e}")

