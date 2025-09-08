import logging

logger = logging.getLogger(__name__)

class Manager:
    def __init__(self,files,kafka_producer):
        self.files = files
        self.kafka_producer = kafka_producer

    async def publish(self,topic_name):
        logger.info("starting to publish messages")
        for f in self.files:
            await self.kafka_producer.publish(topic_name,f)
