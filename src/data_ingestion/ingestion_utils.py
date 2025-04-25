import psycopg2
import socket
import numpy as np
from datetime import datetime
import redis
import json
from logger.logger import setup_logger

logger = setup_logger("ingestion_utils")

def init_questdb_client(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        if conn:
            logger.info("Connected to QuestDB.")
            return conn
    except Exception as e:
        logger.error(f"Error connecting to QuestDB: {e}")
        return None


def create_bronze_layer_qdb(conn, bronze_table, ttl):
    try:
        cur = conn.cursor()

        # Create table with timestamp partitioning
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
            symbol STRING,
            bid DOUBLE,
            bid_qty DOUBLE,
            ask DOUBLE,
            ask_qty DOUBLE,
            last DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            low DOUBLE,
            high DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            timestamp TIMESTAMP
        ) timestamp(timestamp) PARTITION BY DAY
        TTL {ttl}d;
        """

        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Bronze layer table '{bronze_table}' created successfully.")
        cur.close()

    except Exception as e:
        logger.error(f"Failed to create table: {e}")


def create_silver_layer_qdb(conn, silver_table, ttl):
    try:
        cur = conn.cursor()

        # Create table with timestamp partitioning
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            timestamp TIMESTAMP,
            symbol SYMBOL,
            bid DOUBLE,
            bid_qty DOUBLE,
            ask DOUBLE,
            ask_qty DOUBLE,
            last DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            low DOUBLE,
            high DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            ma_5 DOUBLE,
            ma_14 DOUBLE,
            ema_5 DOUBLE,
            std_14 DOUBLE,
            price_change DOUBLE,
            price_change_pct DOUBLE,
            max_14 DOUBLE,
            min_14 DOUBLE,
            vwap_diff DOUBLE,
            bid_ask_spread DOUBLE,
            log_return DOUBLE,
            momentum DOUBLE,
            volatility DOUBLE,
            cumulative_volume DOUBLE,
            volume_change DOUBLE,
            mean_bid_qty DOUBLE,
            mean_ask_qty DOUBLE
        ) timestamp(timestamp) PARTITION BY DAY
        TTL {ttl}d;
        """

        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Silver layer table '{silver_table}' created successfully.")
        cur.close()

    except Exception as e:
        logger.error(f"Failed to create table: {e}")


def create_gold_view_qdb(conn, gold_view, silver_table, interval, ttl):
    try:
        cur = conn.cursor()

        # Create table with timestamp partitioning
        create_table_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {gold_view} AS (
            SELECT
                timestamp,
                symbol,
                last(bid) AS bid,
                avg(bid_qty) AS bid_qty,
                last(ask) AS ask,
                avg(ask_qty) AS ask_qty,
                last(last) AS last,
                sum(volume) AS volume,
                avg(vwap) AS vwap,
                min(low) AS low,
                max(high) AS high,
                last(change) AS change,
                last(change_pct) AS change_pct,
                avg(ma_5) AS ma_5,
                avg(ma_14) AS ma_14,
                last(ema_5) AS ema_5,
                max(std_14) AS std_14,
                sum(price_change) AS price_change,
                sum(price_change_pct) AS price_change_pct,
                max(max_14) AS max_14,
                min(min_14) AS min_14,
                avg(vwap_diff) AS vwap_diff,
                avg(bid_ask_spread) AS bid_ask_spread,
                sum(log_return) AS log_return,
                avg(momentum) AS momentum,
                max(volatility) AS volatility,
                sum(cumulative_volume) AS cumulative_volume,
                avg(volume_change) AS volume_change,
                avg(mean_bid_qty) AS mean_bid_qty,
                avg(mean_ask_qty) AS mean_ask_qty
            FROM {silver_table}
            SAMPLE BY {interval}m ALIGN TO CALENDAR
        ) PARTITION BY HOUR TTL {ttl} HOURS;

        """

        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Gold View '{gold_view}' created successfully.")
        cur.close()

    except Exception as e:
        logger.error(f"Failed to create table: {e}")


def create_gold_bid_pred_layer_qdb(conn, gold_table, ttl):
    try:
        cur = conn.cursor()

        # Create table with timestamp partitioning
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            timestamp TIMESTAMP,
            symbol SYMBOL,
            bid DOUBLE,
            bid_qty DOUBLE,
            ask DOUBLE,
            ask_qty DOUBLE,
            last DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            low DOUBLE,
            high DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            ma_5 DOUBLE,
            ma_14 DOUBLE,
            ema_5 DOUBLE,
            std_14 DOUBLE,
            price_change DOUBLE,
            price_change_pct DOUBLE,
            max_14 DOUBLE,
            min_14 DOUBLE,
            vwap_diff DOUBLE,
            bid_ask_spread DOUBLE,
            log_return DOUBLE,
            momentum DOUBLE,
            volatility DOUBLE,
            cumulative_volume DOUBLE,
            volume_change DOUBLE,
            mean_bid_qty DOUBLE,
            mean_ask_qty DOUBLE,
            BID_prediction_1min DOUBLE,
            BID_prediction_2min DOUBLE,
            BID_prediction_3min DOUBLE,
            BID_prediction_4min DOUBLE,
            BID_prediction_5min DOUBLE,
            latency_ms DOUBLE
        ) timestamp(timestamp) PARTITION BY DAY
        TTL {ttl}d;
        """

        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Gold layer table '{gold_table}' created successfully.")
        cur.close()

    except Exception as e:
        logger.error(f"Failed to create table: {e}")



def create_gold_vol_pred_layer_qdb(conn, gold_table, ttl):
    try:
        cur = conn.cursor()

        # Create table with timestamp partitioning
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            timestamp TIMESTAMP,
            symbol SYMBOL,
            bid DOUBLE,
            bid_qty DOUBLE,
            ask DOUBLE,
            ask_qty DOUBLE,
            last DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            low DOUBLE,
            high DOUBLE,
            change DOUBLE,
            change_pct DOUBLE,
            ma_5 DOUBLE,
            ma_14 DOUBLE,
            ema_5 DOUBLE,
            std_14 DOUBLE,
            price_change DOUBLE,
            price_change_pct DOUBLE,
            max_14 DOUBLE,
            min_14 DOUBLE,
            vwap_diff DOUBLE,
            bid_ask_spread DOUBLE,
            log_return DOUBLE,
            momentum DOUBLE,
            volatility DOUBLE,
            cumulative_volume DOUBLE,
            volume_change DOUBLE,
            mean_bid_qty DOUBLE,
            mean_ask_qty DOUBLE,
            VOL_prediction_1min DOUBLE,
            VOL_prediction_2min DOUBLE,
            VOL_prediction_3min DOUBLE,
            VOL_prediction_4min DOUBLE,
            VOL_prediction_5min DOUBLE,
            latency_ms DOUBLE
        ) timestamp(timestamp) PARTITION BY DAY
        TTL {ttl}d;
        """

        cur.execute(create_table_sql)
        conn.commit()
        logger.info(f"Gold layer table '{gold_table}' created successfully.")
        cur.close()

    except Exception as e:
        logger.error(f"Failed to create table: {e}")


def send_ilp_to_questdb_ilp_host(host, port, table_name, features):
    try:
        # Convert fields to ILP line format
        symbol = features["symbol"]
        timestamp = features["timestamp"]

        # Only convert if it's a string
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)

        timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)

        tags = f"symbol={symbol}"
        fields = ",".join([f"{k}={features[k]}" for k in features if k not in ["symbol", "timestamp"]])
        ilp_line = f"{table_name},{tags} {fields} {timestamp_ns}\n"

        # Send over TCP socket to QuestDB ILP port (9009)
        with socket.create_connection((host, port)) as sock:
            sock.sendall(ilp_line.encode())

        logger.info(f"Sent ILP line to QuestDB {table_name}")
    except Exception as e:
        logger.error(f"Failed to send ILP line: {e}")



# feature engineering functions
def round_features(features, precision=5):
    return {
        k: round(v, precision) if k != "timestamp" and isinstance(v, (float, int)) and v is not None else v
        for k, v in features.items()
    }


def compute_features(price_buffer, symbol, row):
    buffer = price_buffer[symbol]
    buffer.append(row)

    if len(buffer) < 14:
        return None, None

    closes = np.array([r['last'] for r in buffer])
    volumes = np.array([r['volume'] for r in buffer])
    bid_qtys = np.array([r['bid_qty'] for r in buffer])
    ask_qtys = np.array([r['ask_qty'] for r in buffer])

    timestamp = row["timestamp"].isoformat()
    bid = row["bid"]
    bid_qty = row["bid_qty"]
    ask = row["ask"]
    ask_qty = row["ask_qty"]
    last = row["last"]
    volume = row["volume"]
    vwap = row["vwap"]
    low = row["low"]
    high = row["high"]
    change = row["change"]
    change_pct = row["change_pct"]
    ma_5 = np.mean(closes[-5:])
    ma_14 = np.mean(closes[-14:])
    ema_5 = np.mean(closes[-5:] * np.linspace(0.5, 1.0, 5))
    std_14 = np.std(closes[-14:])
    price_change = closes[-1] - closes[0]
    price_change_pct = ((closes[-1] - closes[0]) / closes[0]) * 100
    max_14 = np.max(closes[-14:])
    min_14 = np.min(closes[-14:])
    vwap_diff = last - vwap
    bid_ask_spread = ask - bid
    log_return = np.log(closes[-1] / closes[-2]) if closes[-2] > 0 else 0
    momentum = closes[-1] - closes[-5]
    volatility = np.std(np.diff(closes[-14:]))
    cumulative_volume = np.sum(volumes[-14:])
    volume_change = volumes[-1] - volumes[-2]
    mean_bid_qty = np.mean(bid_qtys)
    mean_ask_qty = np.mean(ask_qtys)

    features_to_write = {
        "timestamp": timestamp,
        "symbol": symbol,
        "bid": bid,
        "bid_qty": bid_qty,
        "ask": ask,
        "ask_qty": ask_qty,
        "last": last,
        "volume": volume,
        "vwap": vwap,
        "low": low,
        "high": high,
        "change": change,
        "change_pct": change_pct,
        "ma_5": ma_5,
        "ma_14": ma_14,
        "ema_5": ema_5,
        "std_14": std_14,
        "price_change": price_change,
        "price_change_pct": price_change_pct,
        "max_14": max_14,
        "min_14": min_14,
        "vwap_diff": vwap_diff,
        "bid_ask_spread": bid_ask_spread,
        "log_return": log_return,
        "momentum": momentum,
        "volatility": volatility,
        "cumulative_volume": cumulative_volume,
        "volume_change": volume_change,
        "mean_bid_qty": mean_bid_qty,
        "mean_ask_qty": mean_ask_qty,
    }

    features_for_model = {
        "timestamp": timestamp,
        "bid": bid,
        "bid_qty": bid_qty,
        "ask": ask,
        "ask_qty": ask_qty,
        "last": last,
        "volume": volume,
        "vwap": vwap,
        "low": low,
        "high": high,
        "change": change,
        "change_pct": change_pct,
        "ma_5": ma_5,
        "ma_14": ma_14,
        "ema_5": ema_5,
        "std_14": std_14,
        "price_change": price_change,
        "price_change_pct": price_change_pct,
        "max_14": max_14,
        "min_14": min_14,
        "vwap_diff": vwap_diff,
        "bid_ask_spread": bid_ask_spread,
        "log_return": log_return,
        "momentum": momentum,
        "volatility": volatility,
        "cumulative_volume": cumulative_volume,
        "volume_change": volume_change,
        "mean_bid_qty": mean_bid_qty,
        "mean_ask_qty": mean_ask_qty,
    }

    features = round_features(features_for_model)
    return features_to_write, features


# Redis
class RedisFeatureStore:
    def __init__(self, host, port, db = 0, ttl = 300):
        """
        Initialize the Redis feature store.
        :param host: Redis host
        :param port: Redis port
        :param db: Redis DB index
        :param ttl: Time to live (seconds) for each feature vector
        """
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.ttl = ttl

    def store(self, symbol, feature_vector):
        """
        Store the feature vector for a given symbol with TTL.
        :param symbol: Unique symbol (e.g., 'BTCUSD')
        :param feature_vector: Dictionary of features
        """
        key = f"feature:{symbol}"
        try:
            serialized = json.dumps(feature_vector)
            # print(f"Serialized feature vector: {serialized}")
            self.redis_client.setex(key, self.ttl, serialized)
            self.redis_client.publish(f"feature_update:{symbol}", f"New feature vector for {symbol}")
            logger.info(f"Stored feature vector for {symbol}")

        except Exception as e:
            logger.error(f"Failed to store feature vector for {symbol}: {e}")

    def fetch(self, symbol):
        """
        Fetch the latest feature vector for a given symbol.
        :param symbol: Unique symbol (e.g., 'BTCUSD')
        :return: Dictionary of features or None
        """
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

    def delete(self, symbol):
        """
        Manually delete a symbol's feature vector (if needed).
        :param symbol: Unique symbol
        """
        key = f"feature:{symbol}"
        try:
            self.redis_client.delete(key)
            logger.info(f"Deleted feature vector for {symbol}")
        except Exception as e:
            logger.error(f"Failed to delete feature vector for {symbol}: {e}")



