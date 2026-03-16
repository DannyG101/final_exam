from kafka_consumer import KafkaConsumer
from intel_handler_config import IntelHandlerConfig

config = IntelHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "intel", "intel_handler")

consumer.consume()