import pymongo
import os
import urllib
from pymongo.errors import BulkWriteError
import datetime
from dynamodb_json import json_util as json
from aws_lambda_powertools.utilities import parameters

# define dynamodb hash key and sort key
DB_HASH_KEY = os.environ.get("DB_HASH_KEY")
DB_SORT_KEY = os.environ.get("DB_SORT_KEY")


# define mongodb url, username and password
# secret = (
#     json.loads(parameters.get_secret(os.environ.get("ATLAS_SECRET")))
#     if os.environ.get("ATLAS_SECRET") != None
#     else ""
# )

urlDb = "mongodb://pc2admin:na%2641d1CPC%297toaXEp5%3C%2B-l6ZH%5D%21MO%5DZ3%5B4%3Ef%3C4jw%7Bx%5DD2QP%25GNY9e%5DK%2Bhcrq%7D%282IFLXfv%26%5DS%3AD4-f4zswn%3AHF%2Be%7Bwxo%3D%25%28%24N1I@ircc-pc2-dev-cluster.cluster-cug1sdcjdsgw.ca-central-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"

# urlDb = f"mongodb://{secret["user"]}:{urllib.parse.quote(secret["password"])}@{secret["endpoint"]}:{secret["port"]}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false" if secret != "" else "mongodb://root:pass12345@localhost:27017"
database = os.environ["ATLAS_TABLE"]
table = os.environ["ATLAS_COLLECTION"]


# connect to mongodb
mongo_client = pymongo.MongoClient(urlDb)
database = mongo_client[database]
collection = database[table]

def update_counters(bulk_write_result, counters):
    for key in counters:
        counters[key] += int(bulk_write_result[key])
    return counters  
    
def batch_write(requests):
    print(
        "Batch write started to handled " + str(len(requests)) + " records:: ", requests
    )
    try:
        bulk_write_result = collection.bulk_write(requests)
        print("Batch write result:: ", bulk_write_result)
        return bulk_write_result.bulk_api_result

    except BulkWriteError as bwe:
        print("EXCEPTION:: ", bwe.details)

        for record in bwe.details.writeErrors:
            raise Exception(record)

def record_exists(id):
    return False if collection.find_one({"_id": id})==None else True

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

# Need the new image to update/insert to docDb
def convert_new_image(image):
    conv = json.loads(image)
    if "Item" in conv:
        conv = conv["Item"]

    conv["_id"] = get_index(conv)

    ### custom conversions ###
    conv["sync_at"] = datetime.datetime.now()
    return conv

# Only need the _id to delete the old image
def convert_old_image(image):
    conv = json.loads(image)
    return get_index(conv)
