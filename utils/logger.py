import logging
from elasticsearch import Elasticsearch
from datetime import datetime,timezone
import os

class Logger:
    _logger = None

    @classmethod
    def get_logger(
            cls,
            name="your_logger_name",
            es_host=os.getenv("ES_HOST", "http://localhost:9200"),
            index="logs",
            level=logging.DEBUG
    ):
        if cls._logger:
            return cls._logger

        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            es = Elasticsearch(es_host)
            class ESHandler(logging.Handler):
                def emit(self, record):
                    try:
                        es.index(index=index, document={
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "level": record.levelname,
                            "logger": record.name,
                            "message": record.getMessage()
                        })
                    except Exception as e:
                        print(f"ES log failed: {e}")
            logger.addHandler(ESHandler())
            logger.addHandler(logging.StreamHandler())
            cls._logger = logger
            return logger