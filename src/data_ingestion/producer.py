import asyncio
import json
import websockets
from datetime import datetime
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from ingestion_configs import TRADES_TOPIC, SYMBOL, KRAKEN_WS_URL
from kafka_producer import KafkaDataProducer
from logger.logger import setup_logger

logger = setup_logger("data_ingestion")
logger.info("Starting data ingestion...")

producer = KafkaDataProducer(topic=TRADES_TOPIC)

async def stream_forever():
    while True:
        try:
            logger.info(f"Connecting to WebSocket: {KRAKEN_WS_URL}")
            async with websockets.connect(KRAKEN_WS_URL) as ws:
                subscribe_message = {
                    "method": "subscribe",
                    "params": {
                        "channel": "ticker",
                        "symbol": [SYMBOL]
                    }
                }
                await ws.send(json.dumps(subscribe_message))
                logger.info("Subscribed to ticker channel.")

                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)

                        if isinstance(data, dict) and data.get("channel") == "ticker":
                            data_to_send = data["data"][0]
                            timestamp = datetime.now().isoformat(timespec='milliseconds')
                            data_to_send["timestamp"] = timestamp
                            producer.send_data(data_to_send)
                            logger.info(f"Received data: {data_to_send}")
                    except Exception as e:
                        logger.error(f"Error in receiving/parsing data: {e}")
                        break

        except Exception as conn_err:
            logger.error(f"WebSocket connection error: {conn_err}")

        logger.info("Reconnecting after 5 seconds...")
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(stream_forever())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        producer.close()
        logger.info("Kafka producer closed. Exiting.")
