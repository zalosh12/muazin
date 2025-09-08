import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaProducer
import config


logger = logging.getLogger(__name__)

# KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_BROKER_URL = config.KAFKA_BROKER_URL

class KafkaProducer:
    def __init__(self):
        self.producer = None

    async def start_producer(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v,default=str).encode("utf-8")
        )
        await self.producer.start()
        logger.info("Kafka producer started")

    async def stop_producer(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def publish(self,topic: str, message: dict):
        if self.producer is None:
            raise RuntimeError("Producer not initialized")
        await self.producer.send_and_wait(topic, message)
        logger.info(f"Sent message to {topic}: {str(message)[:30]}...")