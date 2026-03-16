from kafka_consumer import KafkaConsumer
from validation import Validator
from kafka_publisher import KafkaPublisher
from intel_handler_config import IntelHandlerConfig
from orchestrator import Orchestrator

config = IntelHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "intel", "intel_handler")

validator = Validator()

publisher = KafkaPublisher(config.kafka_bootstrap_servers, "intel_signals_dlq")

orchestrator = Orchestrator(consumer, validator, publisher)

orchestrator.run()