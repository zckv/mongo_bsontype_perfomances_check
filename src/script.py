import os
import bson
import json
import random
import logging

from time import time
from pymongo import MongoClient, IndexModel
from os.path import isdir, isfile, abspath, expanduser, expandvars

# Get logger
logger = logging.getLogger(__name__)



class TestUnit:
    def __init__(self, addr, n, path):
        # default values
        self.mongo_addr = addr
        self.n_documents = n
        self.result_path = path

        self.results = {}
        self.result_path = abspath(expanduser(expandvars(self.result_path)))
        client = MongoClient(self.mongo_addr)
        client.drop_database("perfs")
        self.db = client["perfs"]

    def __call__(self):
        data = [random.randint(-2147483648, 2147483647) for _ in range(int(self.n_documents))]
        types = {
                "int": int,
                "long": bson.int64.Int64,
                "decimal": lambda a: bson.decimal128.Decimal128(str(a)),
                "double": float,
                "number": int,
        }
        # Collection with empty validator
        name = f"test"
        self.create_collection(name, {})
        self.fill_collection(name, int, data)
        
        # Collections with validator
        for bsonType, f in types.items(): 
            name = bsonType
            validator = self.create_validator_strict(bsonType)
            self.create_collection(name, validator)
            self.fill_collection(name, f, data)
        # Find values that may or may not be in data
        self.find_test(("test", *types.keys()), range(1000)) 
        # Check size of collections
        self.check_size(("test", *types.keys())) 
        self.save_results()


    def create_collection(self, name, validator=None):
        self.db.create_collection(
            name,
            validator=validator
        )
        self.db[name].create_index("value")

    def create_validator_strict(self, bsonType: str):
        return {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["value", "_id"],
                "properties": {"value": {"bsonType": bsonType}, "_id": {"bsonType": "objectId"}},
                "additionalProperties": False,
            }
        }


    def fill_collection(self, collection:str, function, data):
        # Convert data to right type
        data = [{"value": function(d)} for d in data]
        t0 = time()
        self.db[collection].insert_many(data)
        self.results[collection] = {"insertion": time()-t0}

    def find_test(self, collections, to_find):
        for col in collections:
            t0 = time()
            for i in to_find:
                self.db[col].find({"value": i})
            self.results[col]["find"] = time()-t0

    def check_size(self, collections):
        for col in collections:
            stats = self.db.command("collstats", col)
            self.results[col]["size"] = stats['size']
            self.results[col]["storageSize"] = stats['storageSize']
            self.results[col]["indexSizes"] = sum(stats['indexSizes'].values())
            self.results[col]["totalSize"] = stats['totalSize']

    def save_results(self) -> None:
        if isfile(self.result_path):
            with open(self.result_path, "rt") as old:
                full_results = json.load(old)
        else:
            full_results = {}
        full_results[str(self.n_documents)] = self.results

        with open(self.result_path, "wt", encoding="utf-8") as file:
            json.dump(full_results, file, indent=4)
        logger.info(f"Results were saved in {self.result_path}.")


def get_config(conf):
    for var in conf.keys():
        if var.upper() in os.environ:
            logger.info(f"Use value from env for {var.upper()}: {os.environ[var.upper()]}")
            conf[var] = os.environ[var.upper()]
        else:
            logger.info(f"Use default value for {var.upper()}")



if __name__ == "__main__":
    # default values
    conf = {
        "mongo_addr": "127.0.0.1:27017",
        "logging": logging.WARNING,
        "n_documents": 10,
        "result_path": "./result.json"
    }

    get_config(conf)
    logging.basicConfig(level=int(conf["logging"]))
    TestUnit(
        conf["mongo_addr"],
        conf["n_documents"],
        conf["result_path"],
    )()

