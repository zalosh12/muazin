import os

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL","localhost:9092")
DIRECTORY_PATH = os.getenv("DIRECTORY_PATH",r"C:\Users\eliwa\podcasts_project\podcasts")