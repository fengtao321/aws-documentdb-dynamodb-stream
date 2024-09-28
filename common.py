import pymongo
import os
import urllib
from pymongo.errors import BulkWriteError
import datetime
from dynamodb_json import json_util as json
from aws_lambda_powertools.utilities import parameters

# define mongodb url, username and password

secret = (
    json.loads(parameters.get_secret(os.environ.get("ATLAS_SECRET")))
    if os.environ.get("ATLAS_SECRET") != None
    else ""
)

urlDb = (
    (
        "mongodb://"
        + secret["user"]
        + ":"
        + urllib.parse.quote(secret["password"])
        + os.environ.get("ATLAS_URI")
    )
    if os.environ.get("ATLAS_URI") and secret != ""
    else "mongodb://root:pass12345@localhost:27017"
)
database = (
    os.environ["ATLAS_TABLE"] if os.environ.get("ATLAS_TABLE") else "test_database"
)
table = (
    os.environ["ATLAS_COLLECTION"]
    if os.environ.get("ATLAS_COLLECTION")
    else "test_collection"
)

# define dynamodb hash key and sort key
DB_HASH_KEY = (
    os.environ.get("DB_HASH_KEY") if os.environ.get("DB_HASH_KEY") else "calc_year"
)
DB_SORT_KEY = (
    os.environ.get("DB_SORT_KEY")
    if os.environ.get("DB_SORT_KEY")
    else "calculation_date"
)

# connect to mongodb
mongo_client = pymongo.MongoClient(urlDb)
database = mongo_client[database]
collection = database[table]


def insert_many(requests):
    try:
        response = collection.insert_many(requests)
        print(response)
    except BulkWriteError as bwe:
        print("EXCEPTION:: ", bwe)


def batch_write(requests, records):
    print(
        "Batch write started to handled " + str(len(records)) + " records:: ", records
    )
    try:
        response = collection.bulk_write(requests)
        result = response.bulk_api_result
        print("Batch write result:: ", result)

        if len(result["writeConcernErrors"]) > 0:
            print(result.bulk_api_result)
            raise Exception(result.bulk_api_result)

        if len(result["writeErrors"]) > 0:
            print(result.writeErrors)
            raise Exception(result.writeErrors)
        return (
            int(result["nModified"])
            + int(result["nRemoved"])
            + int(result["nUpserted"])
            + int(result["nInserted"])
        )

    except BulkWriteError as bwe:
        print("EXCEPTION:: ", bwe)

        for record in records:
            raise Exception(record)


def get_index(image):
    hash_key = (
        image[DB_HASH_KEY]
        if isinstance(image[DB_HASH_KEY], str)
        else str(image[DB_HASH_KEY])
    )
    sort_key = (
        image[DB_SORT_KEY]
        if isinstance(image[DB_SORT_KEY], str)
        else str(image[DB_SORT_KEY])
    )
    return hash_key + "||" + sort_key


def convert_new_image(image):
    conv = json.loads(image)
    if "Item" in conv:
        conv = conv["Item"]

    conv["_id"] = get_index(conv)

    ### custom conversions ###
    conv["created_at"] = datetime.datetime.now()
    return conv


def convert_old_image(image):
    conv = json.loads(image)
    return get_index(conv)
