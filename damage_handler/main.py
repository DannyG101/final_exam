from kafka_consumer import KafkaConsumer
from damage_handler_config import DamageHandlerConfig
from orchestrator import Orchestrator

config = DamageHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "damage", "damage_handler")

orchestrator = Orchestrator(consumer)

orchestrator.run()