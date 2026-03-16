from logger import log_event

class Orchestrator:
    def __init__(self, consumer, validator, publisher, mongo_connection):
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
            attack_id = valid_report["attack_id"]
            mongo_search = self.mongo_connection.find_in_mongo_by_attack_id(attack_id)

            if mongo_search:
                self.mongo_connection.update_mongo_by_attack_id(attack_id, valid_report)
            else:
                log_event("WARNING", "No data found for this attack_id")

    def run(self):
        while True:
            self.handle_single_event()