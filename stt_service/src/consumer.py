import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from utils.logger import Logger

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("KafkaConsumer")
logger = Logger.get_logger()

KAFKA_BROKER_URL= "localhost:9092"
# KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")



class KafkaConsumer:
    def __init__(self):
        self.consumer = None
        self.topic = os.getenv("TOPIC_NAME","podcasts_data_to_transcribe")

    async def start_consumer(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            group_id="stt_group",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        await self.consumer.start()
        logger.info(f"Kafka consumer started, subscribed to: {self.consumer.subscription()}")

    async def stop_consumer(self):
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def get_messages(self):
        if self.consumer is None:
            raise RuntimeError("Consumer not initialized")
        logger.info("Waiting for messages...")
        try:
            async for msg in self.consumer:
                logger.info(f"Received message from topic '{msg.topic}': {msg.value}")
                yield msg.value
        except Exception as e:
            logger.error(f"Error while consuming: {e}")