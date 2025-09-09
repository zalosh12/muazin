import asyncio
from persister.src.db_handler import MongoDB
from persister.src.consumer import KafkaConsumer
from persister.src.producer import KafkaProducer
from persister_manager import Persister
# from persister.src.es_handler import EsClient
from utils.logger import Logger


logger = Logger.get_logger()

async def main() :
    mongo = MongoDB()
    kafka_consumer = KafkaConsumer()
    kafka_producer = KafkaProducer()

    # Create a Future that will never resolve, keeping the event loop alive
    # This will be cancelled when KeyboardInterrupt or other exception occurs
    stop_event = asyncio.Event()

    try:
        # Connect to MongoDB first
        await mongo.connect()
        # Then start Kafka consumer
        await kafka_consumer.start_consumer()
        # start kafka producer
        await kafka_producer.start_producer()


        persister = Persister(db=mongo,
                              kafka_producer=kafka_producer,
                              kafka_consumer=kafka_consumer)

        # Start the consumer and persister task in the background
        # This allows the main coroutine to then wait on the stop_event
        persister_task = asyncio.create_task(persister.consume_and_persist())

        # Wait indefinitely, keeping the event loop alive
        # This will be cancelled by KeyboardInterrupt
        await stop_event.wait()

    except KeyboardInterrupt :
        logger.info("Application shutting down due to KeyboardInterrupt...")
    except Exception as e :
        logger.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
    finally :
        logger.info("Initiating graceful shutdown of services...")
        # Ensure the persister task is cancelled if it's still running
        if 'persister_task' in locals() and not persister_task.done():
            persister_task.cancel()
            try:
                await persister_task # Await to allow proper cleanup within the task if needed
            except asyncio.CancelledError:
                logger.info("Persister task successfully cancelled.")
            except Exception as e:
                logger.error(f"Error during persister task cancellation: {e}", exc_info=True)
            finally:
                await kafka_consumer.stop_consumer()
                await kafka_producer.stop_producer()
                mongo.close()

        await kafka_consumer.stop_consumer()
        await kafka_producer.stop_producer()
        mongo.close()
        logger.info("Application gracefully shut down.")


if __name__ == "__main__" :
    asyncio.run(main())