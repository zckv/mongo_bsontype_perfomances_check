import os
import bson
import random
import logging

from time import time
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING

# Get logger
logger = logging.getLogger(__name__)

# default values
config = {
    "mongo_addr": "127.0.0.1:27017",
    "logging": logging.INFO,
    "n_documents": 10,
}

def get_config():
    for var in config.keys():
        if var.upper() in os.environ:
            logger.info(f"Use value from env for {var.upper()}: {os.environ[var.upper()]}")
            config[var] = os.environ[var.upper()]
        else:
            logger.info(f"Use default value for {var.upper()}")

def create_collection(db, name, validator=None):
    db.create_collection(
        name,
        validator=validator
    )
    db[name].create_index("value")

def create_validator_strict(bsonType: str):
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["value", "_id"],
            "properties": {"value": {"bsonType": bsonType}, "_id": {"bsonType": "objectId"}},
            "additionalProperties": False,
        }
    }


def fill_collection(db, collection:str, function, data):
    # Convert data to right type
    data = [{"value": function(d)} for d in data]
    t0 = time()
    db[collection].insert_many(data)
    logging.info(f"{collection} insert: {time()-t0} sec.")

def find_test(db, collections, to_find):
    for col in collections:
        t0 = time()
        for i in to_find:
            db[col].find({"value": i})
        logging.info(f"{col} find: {time()-t0} sec.")

def check_size(db, collections):
    for col in collections:
        stats = db.command("collstats", col)
        logging.info(f"{col} size: {stats['size']}")
        logging.info(f"{col} storage size: {stats['storageSize']}")
        logging.info(f"{col} indexes size: {sum(stats['indexSizes'].values())}")
        logging.info(f"{col} total size: {stats['totalSize']}")

def main(config):
    client = MongoClient(config["mongo_addr"])
    client.drop_database("perfs")
    db = client["perfs"]
    data = [random.randint(-2147483648, 2147483647) for _ in range(int(config["n_documents"]))]

    types = {
            "int": int,
            "long": bson.int64.Int64,
            "decimal": lambda a: bson.decimal128.Decimal128(str(a)),
            "double": float,
            "number": int,
    }

    # Collection with empty validator
    name = f"test"
    create_collection(db, name, {})
    fill_collection(db, name, int, data)
    
    # Collections with validator
    for bsonType, f in types.items(): 
        name = bsonType
        validator = create_validator_strict(bsonType)
        create_collection(db, name, validator)
        fill_collection(db, name, f, data)
    
    # Find values that may or may not be in data
    find_test(db, ["test", *types.keys()], range(1000)) 

    # Check size of collections
    check_size(db, ["test", *types.keys()]) 

 

if __name__ == "__main__":
    get_config()
    logging.basicConfig(level=config["logging"])
    main(config)
