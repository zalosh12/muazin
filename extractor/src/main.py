from extractor.src.loader import Loader
from extractor.src.manager import Manager
from extractor.src.producer import KafkaProducer
import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO)


async def main():
    loader = Loader(directory_path=os.getenv("DIRECTORY_PATH",r"C:\Users\eliwa\podcasts_project\podcasts"))
    docs = loader.extract_meta_data()

    kafka = KafkaProducer()
    await kafka.start_producer()

    manager = Manager(docs,kafka)
    await manager.publish("podcasts_data")
    await kafka.stop_producer()


if __name__ == "__main__" :
    asyncio.run(main())
