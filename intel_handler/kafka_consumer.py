from json import JSONDecodeError

from confluent_kafka import Consumer
import json

class KafkaConsumer:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    def consume(self):
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest"
        }

        consumer = Consumer(consumer_config)

        consumer.subscribe([self.topic])

        print(f"Consumer running and subscribed to {self.topic}")
        # self.logger.info(f"Consumer running and subscribed to {self.topic}")

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    # self.logger.error("Error:", msg.error())
                    continue

                value = msg.value().decode("utf-8")
                try:
                    data = json.loads(value)
                    print(data)
                except JSONDecodeError as e:
                    print(f"error: {e}")

        except KeyboardInterrupt:
            # self.logger.info("consumer stopped")
            print("Keyboard stop")

        finally:
            consumer.close()


