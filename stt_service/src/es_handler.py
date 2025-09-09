from elasticsearch import AsyncElasticsearch
from utils.logger import Logger
from stt_service.src.config import ES_HOST

# logger = logging.getLogger(__name__)

logger = Logger.get_logger()

mappings = {
    "properties": {
        "path": {"type": "keyword"},
        "file_size": {"type": "keyword"},
        "file_name": {"type": "keyword"},
        "created_at": {"type": "keyword"},
        "file_id":{"type": "keyword"},
        "transcript":{"type": "text"}
    }
}




class EsClient:
    def __init__(self,index_name="podcasts_data_transcript"):
        self.index_name = index_name
        self.client = None

    async def connect(self):
        try:
            logger.info("connecting to elasticsearch")
            self.client = AsyncElasticsearch(ES_HOST)

        except Exception as e:
            logger.error(f"failed to connect to elasticsearch:{e}",exc_info=True)


    async def create_index(self) :
        await self.client.indices.create(index=self.index_name, ignore=400,mappings=mappings)

    async def index_doc(self,doc, doc_id):
        await self.client.index(index=self.index_name, id=doc_id, document=doc)
        logger.info(f"Document indexed in {doc_id}")

    async def close(self) :
        if self.client :
            await self.client.close()
            logger.info("Elasticsearch connection closed")