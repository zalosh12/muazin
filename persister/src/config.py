import os


MDB_USER = os.getenv("MDB_USER", "root")
MDB_PASSWORD = os.getenv("MDB_PASSWORD", "example")
MDB_HOST = os.getenv("MDB_HOST", "localhost:27017")
DB_NAME = os.getenv("DB_NAME", "podcasts_data")

MDB_URI = f"mongodb://{MDB_USER}:{MDB_PASSWORD}@{MDB_HOST}/{DB_NAME}?authSource=admin"


KAFKA_BROKER_URL ="localhost:9092"
# KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
# KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

ES_HOST = "http://localhost:9200"
ES_API_KEY = "dnU2OEZKa0JCdTRNMlhuRkxwejI6eldGdmQwTDkza1VMbHpPYzg3ZkFUQQ"
ES_CLOUD_ID = "zalosh:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRhNTc1MWUyOTUzMmQ0NGMzOWI3M2U2NDI1ZjBiZTNhNSRjYjI4ZDUwZGQ5NmU0YTg0YjJkZTA5YmRiMzI1NzViOQ=="