from kafka_consumer import KafkaConsumer
from intel_handler_config import IntelHandlerConfig
from orchestrator import Orchestrator

config = IntelHandlerConfig()

consumer = KafkaConsumer(config.kafka_bootstrap_servers, "intel", "intel_handler")

orchestrator = Orchestrator(consumer)

orchestrator.run()