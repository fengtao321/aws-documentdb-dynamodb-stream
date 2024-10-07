import json
import os
from dynamodb_json import json_util as json
from pymongo import DeleteOne, UpdateOne
from common import (
    batch_write,
    convert_new_image,
    convert_old_image,
    record_exists,
    update_counters,
)

batch_size = int(os.environ["BATCH_SIZE"]) if os.environ.get("BATCH_SIZE") else 1


def lambda_handler(event, context):

    # the keys are same as the keys in the result after calling collection.bulk_write
    counters = {
        "nModified": 0,
        "nRemoved": 0,
        "nUpserted": 0,
        "nInserted": 0,
    }
    db_write_requests = []

    print("Start to handle records:: ", event["Records"])

    for record in event["Records"]:
        # update the database write requests
        try:
            # Step one:: if the even is INSERT or MODIFY, get the new record form dynamodb new image
            if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
                new_image_conv = convert_new_image(record["dynamodb"]["NewImage"])
                db_write_requests.append(
                    UpdateOne(
                        {"_id": new_image_conv["_id"]},
                        {"$set": new_image_conv},
                        upsert=True,
                    )
                )

            # Step two:: if the even is INSERT or MODIFY, get the index from dynamodb old image.
            # Check the existence of the index in doc db. If exists, then remove; otherwise, raise the exception. The record will be in dead letter queue
            elif record["eventName"] == "REMOVE":
                id = convert_old_image(record["dynamodb"]["OldImage"])

                if record_exists(id):
                    db_write_requests.append(DeleteOne({"_id": id}))
                else:
                    raise Exception(record)

            # Step three:: if event name is not in record, raise the exception. The record will be in dead letter queue
            else:
                raise Exception(record)
        except Exception as e:
            print("EXCEPTION:: ", e)

        # Check the length of the doc db write requests, if it is same as the batch size which is defined in env variable,
        # call batch write, and write all the requests sequentially. After write the doc db, update the counters that process successfully.
        if len(db_write_requests) == batch_size:
            bulk_write_result = batch_write(db_write_requests)
            counters = update_counters(bulk_write_result, counters)
            db_write_requests = []

    # after handled all records, call batch write and update counters
    if len(db_write_requests) > 0:
        bulk_write_result = batch_write(db_write_requests)
        counters = update_counters(bulk_write_result, counters)

    # return response code to Lambda and log on CloudWatch
    if sum(counters.values()) == len(event["Records"]):
        print(
            "Successfully processed %s records:: " % str(len(event["Records"])),
            counters,
        )
        return {"statusCode": 200, "body": json.dumps("OK")}
    else:
        print(
            "Processed only ",
            str(sum(counters.values())),
            " records on %s :: " % str(len(event["Records"]), counters),
        )
        return {"statusCode": 500, "body": json.dumps("ERROR")}
