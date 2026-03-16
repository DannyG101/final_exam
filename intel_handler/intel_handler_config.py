import os
from dotenv import load_dotenv

load_dotenv()


class IntelHandlerConfig:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


    def validate_environment_vars(self):
        if not self.kafka_bootstrap_servers:
            return False
        return True

