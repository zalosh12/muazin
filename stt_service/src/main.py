import asyncio
from stt_service.src.db_handler import MongoDB
from stt_service.src.consumer import KafkaConsumer
from stt_service.src.es_handler import EsClient
from stt_service.src.transcriber_manager import TranscriberManager
from stt_service.src.analyzer import AnalyzeText
from stt_service.src.config import high_hostile_words,low_hostile_words
from utils.logger import Logger




logger = Logger.get_logger()

async def main() :
    mongo = MongoDB()
    kafka = KafkaConsumer()
    es = EsClient()
    text_analyzer = AnalyzeText(high_hostile_words,low_hostile_words)

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

        await es.create_index()

        transcriber = TranscriberManager(mongo, kafka,es,text_analyzer)

        # Start the consumer and transcriber task in the background
        # This allows the main coroutine to then wait on the stop_event
        transcriber_task = asyncio.create_task(transcriber.consume_and_transcribe())

        # Wait indefinitely, keeping the event loop alive
        # This will be cancelled by KeyboardInterrupt
        await stop_event.wait()

    except KeyboardInterrupt :
        logger.info("Application shutting down due to KeyboardInterrupt...")
    except Exception as e :
        logger.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
    finally :
        logger.info("Initiating graceful shutdown of services...")
        # Ensure the transcriber task is cancelled if it's still running
        if 'transcriber_task' in locals() and not transcriber_task.done():
            transcriber_task.cancel()
            try:
                await transcriber_task # Await to allow proper cleanup within the task if needed
            except asyncio.CancelledError:
                logger.info("Transcriber task successfully cancelled.")
            except Exception as e:
                logger.error(f"Error during transcriber task cancellation: {e}", exc_info=True)

        await kafka.stop_consumer()
        mongo.close()
        await es.close()
        logger.info("Application gracefully shut down.")


if __name__ == "__main__" :
    asyncio.run(main())