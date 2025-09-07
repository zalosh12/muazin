from loader import Loader
from manager import Manager
from producer import KafkaProducer
import os
import asyncio


async def main():
    loader = Loader(os.getenv("DIRECTORY_PATH",r"C:\Users\eliwa\podcasts_project\podcasts"))
    docs = loader.extract_meta_data()

    kafka = KafkaProducer()
    await kafka.start_producer()

    manager = Manager(docs,kafka)
    await manager.publish("podcasts_data")
    await kafka.stop_producer()


if __name__ == "__main__" :
    asyncio.run(main())
