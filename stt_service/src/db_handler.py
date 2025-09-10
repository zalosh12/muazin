from motor.motor_asyncio import AsyncIOMotorClient
from stt_service.src.config import MDB_URI, DB_NAME
from utils.logger import Logger


logger = Logger.get_logger()

class MongoDB :
    def __init__(self) :
        self.client = None
        self.db = None

    async def connect(self) :
        try:
            logger.info("Connecting to MongoDB...")
            self.client = AsyncIOMotorClient(MDB_URI)
            await self.client.admin.command('ismaster')
            self.db = self.client[DB_NAME]
            logger.info(f"Successfully connected to MongoDB database: {DB_NAME}")
        except Exception as e :
            logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)


    def close(self) :
        if self.client :
            self.client.close()
            logger.info("MongoDB connection closed")