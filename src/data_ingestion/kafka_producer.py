import json
import os
from datetime import datetime
from kafka import KafkaProducer
from ingestion_configs import KAFKA_BROKER
from logger.logger import setup_logger

logger = setup_logger("kafka_producer")
logger.info("Kafka Producer logger initialized.")


class KafkaDataProducer:
    """Generic Kafka Producer to send data to a Kafka topic."""
    
    def __init__(self, topic, broker=KAFKA_BROKER):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_data(self, data):
        """Send JSON data to Kafka topic."""
        try:
            self.producer.send(self.topic, value=data)
            self.producer.flush()
            logger.info(f"Data sent to Kafka topic '{self.topic}'- {datetime.now()}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")

    def close(self):
        """Close the Kafka producer."""
        self.producer.close()