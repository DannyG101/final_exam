from confluent_kafka import Producer
import json
from logger import log_event

class KafkaPublisher:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

    def produce(self, report):
        producer_config = {
            "bootstrap.servers": self.kafka_bootstrap_servers
        }

        producer = Producer(producer_config)

        def delivery_report(err, msg):
            if err:
                log_event("ERROR", f"Delivery failed: {err}")
            else:
                log_event("WARNING", f"✅ Delivered {msg.value().decode("utf-8")}")


        value = json.dumps(report).encode("utf-8")

        producer.produce(
            topic=self.kafka_topic,
            value=value,
            callback=delivery_report
        )

        producer.flush()



