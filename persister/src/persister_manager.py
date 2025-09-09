import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
import aiofiles
from utils.logger import Logger

logger = Logger.get_logger()
# logger = logging.getLogger(__name__)

class Persister:
    def __init__(self, db, kafka, es_client):
        self.db = db
        self.kafka = kafka
        self.es = es_client
        # self.collection = "meta_data_and_files"

    async def consume_and_persist(self):
        """ consume messages from kafka and create unique id
        for each document load the file content from path and store it
        in mongo db and indexing the metadata document in elasticsearch"""
        logger.info("Starting message consumption and persistence...")

        fs = AsyncIOMotorGridFSBucket(self.db.db)

        async for msg in self.kafka.get_messages():
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


                # indexing a document into elasticsearch
                await self.es.index_doc(doc_id=_id,doc=msg)

            except Exception as e:
                logger.error(f"Error occured {e}")


