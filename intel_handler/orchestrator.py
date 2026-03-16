
from haversine import haversine_km

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
            signal_id = valid_report["signal_id"]
            mongo_search = self.mongo_connection.find_in_mongo_by_signal_id(signal_id)

            if mongo_search:
                lat1 = mongo_search["reported_lat"]
                lon1 = mongo_search["reported_lon"]
                lat2 = valid_report["reported_lat"]
                lon2 = valid_report["reported_lon"]
                valid_report["distance"] = haversine_km(lat1, lon1, lat2, lon2)
                self.mongo_connection.update_mongo_by_signal_id(signal_id, valid_report)
            else:
                valid_report["distance"] = 0
                self.mongo_connection.write_to_mongo(valid_report)

    def run(self):
        while True:
            self.handle_single_event()