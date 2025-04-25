import threading
import redis
import requests
import json
import time
import socket
from datetime import datetime
from logger.logger import setup_logger
from downstream_configs import SYMBOL

logger = setup_logger("redis-consumer")

class RedisFeatureFetcher:
    def __init__(self, host, port, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def fetch(self, symbol):
        key = f"feature:{symbol}"
        try:
            serialized = self.redis_client.get(key)
            if serialized:
                feature_vector = json.loads(serialized)
                return feature_vector
            else:
                return None
        except Exception as e:
            logger.error(f"Error fetching feature vector for {symbol}: {e}")
            return None

def send_ilp_to_questdb_ilp_host(host, port, table_name, features, predictions, task):
    try:
        # Parse the timestamp to datetime and convert to nanoseconds
        timestamp_str = features["timestamp"]
        ts = datetime.fromisoformat(timestamp_str)
        timestamp_ns = int(ts.timestamp() * 1_000_000_000)

        # Add model predictions to the feature dictionary
        pred_1m, pred_2m, pred_3m, pred_4m, pred_5m = predictions
        features[f"{task}_prediction_1min"] = pred_1m
        features[f"{task}_prediction_2min"] = pred_2m
        features[f"{task}_prediction_3min"] = pred_3m
        features[f"{task}_prediction_4min"] = pred_4m
        features[f"{task}_prediction_5min"] = pred_5m

        now = datetime.now()
        latency_ms = (now - ts).total_seconds() * 1000
        features["latency_ms"] = round(latency_ms, 3)

        # Prepare tags and fields
        symbol = SYMBOL
        tags = f"symbol={symbol}"

        # Skip 'symbol' and 'timestamp' since they're handled separately
        fields = ",".join([
            f'{k}={format_value(features[k])}' 
            for k in features if k not in ["symbol", "timestamp"]
        ])

        # Final ILP line
        ilp_line = f"{table_name},{tags} {fields} {timestamp_ns}\n"

        # Send over TCP to QuestDB
        with socket.create_connection((host, port)) as sock:
            sock.sendall(ilp_line.encode())

        logger.info(f"Sent ILP line to QuestDB")

    except Exception as e:
        logger.error(f"Failed to send ILP line: {e}")

def format_value(val):
    if isinstance(val, str):
        return f'"{val}"'
    elif isinstance(val, bool):
        return "true" if val else "false"
    elif val is None:
        return 'null'
    else:
        return val


def send_prediction_request(endpoint, features, raw_msg, host, port, table_name, task):
    try:
        payload = {
            "data": features
        }
        response = requests.post(endpoint, json=payload)
        logger.info(f"Sending prediction request to {endpoint}")
        logger.info(f"Response status code: {response.status_code}")

        if response.status_code == 200:
            # logger.info(f"Prediction from {endpoint}: {response.json()}")
            predictions = response.json()
            pred_list = predictions['prediction'][0]
            
            if len(pred_list) == 5:
                send_ilp_to_questdb_ilp_host(host, port, table_name, raw_msg, pred_list, task)
                logger.info(f"Predictions sent to QuestDB for {task}")
            else:
                logger.warning(f"Unexpected number of predictions from {endpoint}: {len(pred_list)}")
        else:
            logger.warning(f"Failed to get prediction from {endpoint}. Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending prediction request to {endpoint}: {e}")
