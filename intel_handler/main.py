from kafka_consumer import KafkaConsumer
from validation import Validator
from kafka_publisher import KafkaPublisher
from intel_handler_config import IntelHandlerConfig
from mongo_connection import MongoConnection
from orchestrator import Orchestrator


config = IntelHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "intel", "intel_handler")

validator = Validator()

publisher = KafkaPublisher(config.kafka_bootstrap_servers, "intel_signals_dlq")

mongo_connection = MongoConnection(config.mongo_uri)

orchestrator = Orchestrator(consumer, validator, publisher, mongo_connection)

orchestrator.run()