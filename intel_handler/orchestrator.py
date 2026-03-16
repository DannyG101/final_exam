from kafka_consumer import KafkaConsumer

class Orchestrator:
    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer

    def handle_single_event(self):
        data = self.consumer.consume()

    def run(self):
        while True:
            self.handle_single_event()