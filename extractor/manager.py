class Manager:
    def __init__(self,files,kafka_producer):
        self.files = files
        self.kafka_producer = kafka_producer

    def publish(self,topic_name):
        for f in self.files:
            self.kafka_producer.publish(topic_name,f)
