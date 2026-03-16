from kafka_consumer import KafkaConsumer
from attack_handler_config import AttackHandlerConfig
from orchestrator import AttackOrchestrator

config = AttackHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "attack", "attack_handler")

orchestrator = AttackOrchestrator(consumer)

orchestrator.run()