import os
from dotenv import load_dotenv

load_dotenv()


class IntelHandlerConfig:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.mongo_uri = os.getenv("MONGO_URI")


    def validate_environment_vars(self):
        if not self.kafka_bootstrap_servers or not self.mongo_uri:
            return False
        return True



