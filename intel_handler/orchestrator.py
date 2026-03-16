from kafka_consumer import KafkaConsumer
from validation import Validator
from kafka_publisher import KafkaPublisher

class Orchestrator:
    def __init__(self, consumer: KafkaConsumer, validator: Validator, publisher: KafkaPublisher):
        self.consumer = consumer
        self.validator = validator
        self.publisher = publisher

    def handle_single_event(self):
        report = self.consumer.consume()
        report_status = self.validator.validate_report(report)
        if report_status["status"] == False:
            invalid_report = {"report": report_status["report"], "error": report_status["error"]}
            self.publisher.produce(invalid_report)
            print("sent_to_kafka")
        elif report_status["status"] == True:
            print("this needs to get sent to mongo asap")

    def run(self):
        while True:
            self.handle_single_event()