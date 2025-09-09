from motor.motor_asyncio import AsyncIOMotorGridFSBucket
import aiofiles
from utils.logger import Logger
import speech_recognition as sr
import io


logger = Logger.get_logger()

class TranscriberManager:
    def __init__(self, db, kafka, es_client) :
        self.db = db
        self.kafka = kafka
        self.es = es_client
        self.recognizer = sr.Recognizer()

    async def consume_and_transcribe(self):
        logger.info("Starting message consumption and persistence...")

        fs = AsyncIOMotorGridFSBucket(self.db.db)

        async for msg in self.kafka.get_messages() :
            try :
                file_id = msg.get("file_id")
                if not file_id :
                    logger.warning(f"No file_id found in message: {msg}")
                    continue


                stream = await fs.open_download_stream(file_id=file_id)
                data = await stream.read()

                logger.info(f"Downloaded file {file_id}, size={len(data)} bytes")

                transcript = await self._transcribe_audio(data)

                logger.info(f"transcribe file {transcript[:30]}")

                msg['transcript'] = transcript

                await self.es.index_doc(doc_id=msg['file_id'],doc=msg)


        # async for msg in self.kafka.get_messages() :
        #     try :
        #         file_id = msg.get("file_id")
        #         if not file_id :
        #             logger.warning(f"No file_id found in message: {msg}")
        #             continue
        #
        #         stream = await fs.open_download_stream(file_id)
        #         data = await stream.read()
        #         await stream.close()
        #         logger.info(type(data))

            except Exception as e:
                logger.error(f"Error occured {e}")

    async def _transcribe_audio(self, audio_bytes) :
        recognizer = sr.Recognizer()
        try :
            # Convert bytes to a file-like object
            audio_file = io.BytesIO(audio_bytes)

            with sr.AudioFile(audio_file) as source :
                audio = recognizer.record(source)  # read the entire file

            transcript = recognizer.recognize_google(audio, language="en-US")
            logger.info("Audio successfully transcribed.")
            return transcript

        except sr.UnknownValueError :
            logger.warning("Could not understand audio.")
            return ""
        except Exception as e :
            logger.error(f"STT error: {e}", exc_info=True)
            return ""

    # async def _transcribe_audio(self, src_file):
    #         recognizer = sr.Recognizer()
    #         try :
    #             audio_data = sr.AudioFile(src_file)
    #             # with audio_data as source :
    #             #     audio = recognizer.record(source)
    #
    #             transcript = self.recognizer.recognize_google(audio, language="en-US")
    #             logger.info("Audio successfully transcribed.")
    #             return transcript
    #         except sr.UnknownValueError :
    #             logger.warning("Could not understand audio.")
    #             return ""
    #         except Exception as e :
    #             logger.error(f"STT error: {e}", exc_info=True)
    #             return ""




