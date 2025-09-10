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

ES_HOST = os.getenv("ES_HOST","http://localhost:9200")

high_hostile_words = """R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWF
zc2FjcmUsTmFrYmEsRGlzc
GxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixS
ZWZ1Z2VlcyxJQ0MsQkRT"""
low_hostile_words="""RnJlZWRvbSBGbG90aWxsYSxSZXNpc
3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSx
DZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ=="""
