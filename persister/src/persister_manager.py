import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
import aiofiles

logger = logging.getLogger(__name__)

class Persister:
    def __init__(self, db, kafka, es_client):
        self.db = db
        self.kafka = kafka
        self.es = es_client
        self.collection = "meta_data_and_files"
        self.msg_counter = 2

    async def consume_and_persist(self):
        """ consume messages from kafka and create unique id
        for each document load the file content from path and store it
        in mongo db and indexing the metadata document in elasticsearch"""
        logger.info("Starting message consumption and persistence...")
        async for msg in self.kafka.get_messages():
            try:
                _id = f"podcast_number_{self.msg_counter}_{msg['created_at']}"
                fs = AsyncIOMotorGridFSBucket(self.db.db)
                #read the file content in binary format
                try:
                    async with aiofiles.open(msg["path"], 'rb') as f :
                        file_data =  await f.read()
                except FileNotFoundError :
                    logger.error(f"Error: WAV file not found ")
                    continue

                #upload the file content to mongo data base
                file_id = await fs.upload_from_stream_with_id(
                    _id,
                    "test_file",
                    file_data,
                    metadata={"contentType" : "audio/wav"})
                logger.info(f'WAV file {msg.get("path")} uploaded successfully with GridFS ID: {file_id}')

                self.msg_counter += 1

                # indexing a document into elasticsearch
                await self.es.index_doc(doc_id=_id,doc=msg)

            except Exception as e:
                logger.error(f"Error occured {e}")


