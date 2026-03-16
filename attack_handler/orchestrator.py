from kafka_consumer import KafkaConsumer

class AttackOrchestrator:
    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer

    def handle_single_event(self):
        self.consumer.consume()

    def run(self):
        while True:
            self.handle_single_event()