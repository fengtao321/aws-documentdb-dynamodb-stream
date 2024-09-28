import pymongo
import json
import os
import urllib
import datetime
from dynamodb_json import json_util as json

from pymongo import DeleteOne, UpdateOne

from pymongo.errors import BulkWriteError


# question about multiple lambda

# define mongodb url, username and password
# miss the part to get password from secret manage
urlDb = (
    (
        "mongodb://pc2admin:"
        + urllib.parse.quote(
            "na&41d1CPC)7toaXEp5<+-l6ZH]!MO]Z3[4>f<4jw{x]D2QP%GNY9e]K+hcrq}(2IFLXfv&]S:D4-f4zswn:HF+e{wxo=%($N1I"
        )
        + os.environ.get("ATLAS_URI")
    )
    if os.environ.get("ATLAS_URI")
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
batch_size = int(os.environ["BATCH_SIZE"]) if os.environ.get("BATCH_SIZE") else 1

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
    print("INDEX :: " + str(image[DB_HASH_KEY]) + "||" + image[DB_SORT_KEY])
    return str(image[DB_HASH_KEY]) + "||" + image[DB_SORT_KEY]


def convert_new_image(image):
    conv = json.loads(image)
    conv["_id"] = get_index(conv)

    ### custom conversions ###
    conv["created_at"] = datetime.datetime.now()
    return conv


def convert_old_image(image):
    conv = json.loads(image)
    return get_index(conv)


def lambda_handler(event, context):

    print("Start to handle records:: ", event["Records"])
    count = 0
    requests, handled_records = [], []

    for record in event["Records"]:
        handled_records.append(record)
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            new_image_conv = convert_new_image(record["dynamodb"]["NewImage"])
            requests.append(
                UpdateOne(
                    {"_id": new_image_conv["_id"]},
                    {"$set": new_image_conv},
                    upsert=True,
                )
            )
        elif record["eventName"] == "REMOVE":
            id = convert_old_image(record["dynamodb"]["OldImage"])
            requests.append(DeleteOne({"_id": id}))
        else:
            raise Exception(record)

        if len(requests) == batch_size:
            count += batch_write(requests, handled_records)
            requests, handled_records = [], []

    if len(requests) > 0:
        count += batch_write(requests)

    # return response code to Lambda and log on CloudWatch
    if count == len(event["Records"]):
        print("Successfully processed %s records." % str(len(event["Records"])))
        return {"statusCode": 200, "body": json.dumps("OK")}
    else:
        print(
            "Processed only ", str(count), " records on %s" % str(len(event["Records"]))
        )
        return {"statusCode": 500, "body": json.dumps("ERROR")}
