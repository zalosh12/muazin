import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorGridFSBucket

logger = logging.getLogger(__name__)

class Persister:
    def __init__(self, db, kafka):
        self.db = db
        self.kafka = kafka
        self.collection = "meta_data_and_files"
        self.msg_counter = 2

    async def consume_and_persist(self):
        logger.info("Starting message consumption and persistence...")
        async for msg in self.kafka.get_messages():
            logger.info(msg)
            try:
                _id = f"podcast_number_{self.msg_counter}_{msg['created_at']}"
                fs = AsyncIOMotorGridFSBucket(self.db.db)
                with open(msg["path"], 'rb') as f :
                    file_data = f.read()
                file_id = await fs.upload_from_stream_with_id(
                    _id,
                    "test_file",
                    file_data,
                    metadata={"contentType" : "audio/wav"})
                logger.info(f'WAV file {msg.get("path")} uploaded successfully with GridFS ID: {file_id}')
                self.msg_counter += 1
            except Exception as e:
                logger.error(f"Error occured {e}")


