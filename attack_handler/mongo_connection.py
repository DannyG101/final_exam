from pymongo import MongoClient
from logger import log_event



class MongoConnection:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri)
        self.db = self.client["target_bank_db"]
        self.collection = self.db["target_bank_collection"]

    def find_in_mongo_by_entity_id(self, entity_id):
        self.collection.find_one({"signal_id": entity_id})


    def update_mongo_by_entity_id(self, entity_id, document):
        self.collection.update_one({"signal_id": entity_id}, document)
        log_event("INFO", "updated attack report to mongo")
