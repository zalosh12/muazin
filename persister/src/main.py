import asyncio
import logging
from persister.src.db_handler import MongoDB
from persister.src.consumer import KafkaConsumer
from persister_manager import Persister
from persister.src.es_handler import EsClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main() :
    mongo = MongoDB()
    kafka = KafkaConsumer()
    es = EsClient()

    # Create a Future that will never resolve, keeping the event loop alive
    # This will be cancelled when KeyboardInterrupt or other exception occurs
    stop_event = asyncio.Event()

    try:
        # Connect to MongoDB first
        await mongo.connect()
        # Then start Kafka consumer
        await kafka.start_consumer()
        # Connect to elasticsearch
        await es.connect()

        persister = Persister(mongo, kafka,es)

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

        await kafka.stop_consumer()
        mongo.close()
        await es.close()
        logger.info("Application gracefully shut down.")


if __name__ == "__main__" :
    asyncio.run(main())