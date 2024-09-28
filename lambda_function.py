import json
import os
import datetime
from dynamodb_json import json_util as json
from pymongo import DeleteOne, UpdateOne
from common import batch_write, convert_new_image, convert_old_image, get_index

batch_size = int(os.environ["BATCH_SIZE"]) if os.environ.get("BATCH_SIZE") else 1


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

        # This happens when remove an item, but the item is not in document db
        print(
            "Processed only ", str(count), " records on %s" % str(len(event["Records"]))
        )
        return {"statusCode": 500, "body": json.dumps("ERROR")}
