from kafka_consumer import KafkaConsumer
from validation import Validator
from kafka_publisher import KafkaPublisher
from mongo_connection import MongoConnection

class Orchestrator:
    def __init__(self, consumer: KafkaConsumer, validator: Validator, publisher: KafkaPublisher, mongo_connection: MongoConnection):
        self.consumer = consumer
        self.validator = validator
        self.publisher = publisher
        self.mongo_connection = mongo_connection

    def handle_single_event(self):
        report = self.consumer.consume()
        report_status = self.validator.validate_report(report)
        if report_status["status"] == False:
            invalid_report = {"report": report_status["report"], "error": report_status["error"]}
            self.publisher.produce(invalid_report)
        elif report_status["status"] == True:
            valid_report = report_status["report"]
            signal_id = valid_report["signal_id"]
            if self.mongo_connection.find_in_mongo_by_signal_id(signal_id):
                self.mongo_connection.update_mongo_by_signal_id(signal_id, valid_report)
            else:
                self.mongo_connection.write_to_mongo(valid_report)

    def run(self):
        while True:
            self.handle_single_event()