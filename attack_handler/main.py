from kafka_consumer import KafkaConsumer
from attack_handler_config import AttackHandlerConfig
from validation import Validator
from kafka_publisher import KafkaPublisher
from mongo_connection import MongoConnection
from orchestrator import AttackOrchestrator

config = AttackHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "attack", "attack_handler")

validator = Validator()

publisher = KafkaPublisher(config.kafka_bootstrap_servers, "intel_signals_dlq")

mongo_connection = MongoConnection(config.mongo_uri)

orchestrator = AttackOrchestrator(consumer, validator, publisher, mongo_connection)

orchestrator.run()