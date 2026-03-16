from kafka_consumer import KafkaConsumer
from validation import validate_report

class Orchestrator:
    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer

    def handle_single_event(self):
        report = self.consumer.consume()
        report_status = validate_report(report)

    def run(self):
        while True:
            self.handle_single_event()