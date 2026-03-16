from pymongo import MongoClient
from logger import log_event



class MongoConnection:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri)
        self.db = self.client["target_bank_db"]
        self.collection = self.db["target_bank_collection"]

    def find_in_mongo_by_attack_id(self, attack_id):
        self.collection.find_one({"signal_id": attack_id})


    def update_mongo_by_attack_id(self, attack_id, document):
        self.collection.update_one({"attack_id": attack_id}, document)
        log_event("INFO", "updated attack report to mongo")
