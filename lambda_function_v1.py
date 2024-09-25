import os
from pymongo import MongoClient
from dynamodb_json import json_util as json
import urllib


# from aws_requests_auth.aws_auth import AWSRequestsAuth
# from elasticsearch import Elasticsearch
# from elasticsearch import RequestsHttpConnection
# from elasticsearch import helpers

# HOST = os.environ.get('ES_HOST')
# INDEX = os.environ.get('ES_INDEX')
DB_HASH_KEY = os.environ.get("DB_HASH_KEY")
DB_SORT_KEY = os.environ.get("DB_SORT_KEY")
mongo_uri = (
    "mongodb://pc2admin:"
    + urllib.parse.quote(
        "na&41d1CPC)7toaXEp5<+-l6ZH]!MO]Z3[4>f<4jw{x]D2QP%GNY9e]K+hcrq}(2IFLXfv&]S:D4-f4zswn:HF+e{wxo=%($N1I"
    )
    + os.environ.get("ATLAS_URI")
)
client = MongoClient(mongo_uri)
database_name = os.environ.get("ATLAS_TABLE")
col_name = os.environ.get("ATLAS_COLLECTION")


def pushBatch(actions):
    print(client.list_database_names())

    # Name of database
    db = client[database_name]

    # Name of collection
    col = db[col_name]

    # Document to add inside
    document = {"first name": "Anaiya", "last name": "Raisinghani"}

    # Insert document
    result = col.insert_one(document)

    if result.inserted_id:
        return "Document inserted successfully"
    else:
        return "Failed to insert document"


def lambda_handler(event, context):

    eventTypes = ["INSERT", "MODIFY", "REMOVE"]
    actions = []
    ignoredRecordCount = 0

    if "Records" in event:
        records = event["Records"]
        for record in records:
            if record["eventName"] in eventTypes:
                action = (
                    json.loads(record["dynamodb"]["OldImage"])
                    if record["eventName"] == "REMOVE"
                    else json.loads(record["dynamodb"]["NewImage"])
                )
                actions.append(
                    {
                        # '_index': INDEX,
                        "_type": "_doc",
                        "_id": action[DB_HASH_KEY] + ":" + action[DB_SORT_KEY],
                        "_source": action,
                        "_op_type": (
                            "delete" if record["eventName"] == "REMOVE" else "index"
                        ),
                    }
                )
            else:
                ignoredRecordCount += 1
                print(record)
            if len(actions) == 50:
                pushBatch(actions)
                actions = []

    # if len(actions) > 0:
    pushBatch(actions)
    print("Invalid Event records ignored: " + str(ignoredRecordCount))
    print("What is the actions?")
    print(actions)
