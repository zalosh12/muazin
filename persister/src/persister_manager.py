from motor.motor_asyncio import AsyncIOMotorGridFSBucket
import aiofiles
from utils.logger import Logger

logger = Logger.get_logger()


class Persister:
    def __init__(self, db, kafka_producer,kafka_consumer):
        self.db = db
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer

    async def consume_and_persist(self):
        """ consume messages from kafka and create unique id
        for each document load the file content from path and store it
        in mongo db and sending the metadata to kafka"""
        logger.info("Starting message consumption and persistence...")

        fs = AsyncIOMotorGridFSBucket(self.db.db)

        async for msg in self.kafka_consumer.get_messages():
            try:
                _id = f"{msg['created_at']}_{msg['file_size']}"
                #read the file content in binary format
                try:
                    async with aiofiles.open(msg["path"], 'rb') as f :
                        file_data =  await f.read()
                except FileNotFoundError :
                    logger.error(f"Error: WAV file not found ")
                    continue

                #upload the file content to mongo data base
                await fs.upload_from_stream_with_id(
                    _id,
                    msg['file_name'],
                    file_data,
                    metadata={"contentType" : "audio/wav"})
                logger.info(f'WAV file {msg.get("file_name")} uploaded successfully with GridFS ID: {_id}')

                msg['file_id'] = _id

                await self.kafka_producer.publish(topic="podcasts_data_to_transcribe",message=msg)

            except Exception as e:
                logger.error(f"Error occured {e}")


