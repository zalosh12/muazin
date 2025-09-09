from elasticsearch import AsyncElasticsearch
from persister.src.config import ES_API_KEY,ES_CLOUD_ID
from utils.logger import Logger
from config import ES_HOST
import logging

# logger = logging.getLogger(__name__)

logger = Logger.get_logger()




class EsClient:
    def __init__(self,index_name="podcasts_data"):
        self.index_name = index_name
        self.client = None

    async def connect(self):
        try:
            logger.info("connecting to elasticsearch")
            self.client = AsyncElasticsearch(ES_HOST)

        except Exception as e:
            logger.error(f"failed to connect to elasticsearch:{e}",exc_info=True)


    async def create_index(self) :
        await self.client.indices.create(index=self.index_name, ignore=400)

    async def index_doc(self,doc, doc_id):
        await self.client.index(index=self.index_name, id=doc_id, document=doc)
        logger.info(f"Document indexed in {doc_id}")

    async def close(self) :
        if self.client :
            await self.client.close()
            logger.info("Elasticsearch connection closed")

