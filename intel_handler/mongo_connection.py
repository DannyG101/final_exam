from pymongo import MongoClient



class MongoConnection:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri)
        self.db = self.client["target_bank_db"]
        self.collection = self.db["target_bank_collection"]

    def find_in_mongo_by_signal_id(self, signal_id):
        self.collection.find_one({"signal_id": signal_id})


    def write_to_mongo(self, document):
        self.collection.insert_one(document)

    def update_mongo_by_signal_id(self, signal_id, document):
        self.collection.update_one({"signal_id": signal_id}, document)
