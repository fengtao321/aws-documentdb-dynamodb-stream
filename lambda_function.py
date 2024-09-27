import pymongo
import json
import os
import urllib
import datetime
from dynamodb_json import json_util as json

# connect to document db
urlDb = (
    "mongodb://pc2admin:"
    + urllib.parse.quote(
        "na&41d1CPC)7toaXEp5<+-l6ZH]!MO]Z3[4>f<4jw{x]D2QP%GNY9e]K+hcrq}(2IFLXfv&]S:D4-f4zswn:HF+e{wxo=%($N1I"
    )
    + os.environ.get("ATLAS_URI")
)
database = os.environ["ATLAS_TABLE"]
table = os.environ["ATLAS_COLLECTION"]


myclient = pymongo.MongoClient(urlDb)
mydb = myclient[database]
mycol = mydb[table]


# define dynamodb hash key and sort key
DB_HASH_KEY = os.environ.get("DB_HASH_KEY")
DB_SORT_KEY = os.environ.get("DB_SORT_KEY")


def lambda_handler(event, context):

    # read env variables for mongodb connection
    count = 0
    ignoredRecordCount = 0

    with myclient.start_session() as session:

        for record in event["Records"]:
            ddb = record["dynamodb"]
            print(record)

            if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
                newimage = ddb["NewImage"]
                newimage_conv = json.loads(newimage)

                # create the explicit _id
                newimage_conv["_id"] = (
                    str(newimage_conv[DB_HASH_KEY]) + "||" + newimage_conv[DB_SORT_KEY]
                )

                ### custom conversions ###
                newimage_conv["created_at"] = datetime.datetime.now()

                try:
                    mycol.update_one(
                        {"_id": newimage_conv["_id"]},
                        {"$set": newimage_conv},
                        upsert=True,
                        session=session,
                    )
                    count = count + 1

                except Exception as e:
                    # add to DL queue
                    print(
                        "ERROR update _id=",
                        str(newimage_conv[DB_HASH_KEY])
                        + "||"
                        + newimage_conv[DB_SORT_KEY],
                        " ",
                        type(e),
                        e,
                    )
                    raise Exception(record)

            elif record["eventName"] == "REMOVE":

                oldimage = ddb["OldImage"]
                oldimage_conv = json.loads(oldimage)

                try:
                    result = mycol.delete_one(
                        {
                            "_id": str(oldimage_conv[DB_HASH_KEY])
                            + "||"
                            + oldimage_conv[DB_SORT_KEY]
                        },
                        session=session,
                    )
                    count = count + 1

                    if result.deleted_count == 0:
                        raise Exception(record)
                except Exception as e:
                    # add to DL queue
                    print(
                        "ERROR delete _id",
                        str(oldimage_conv[DB_HASH_KEY])
                        + "||"
                        + oldimage_conv[DB_SORT_KEY],
                        " ",
                        type(e),
                        e,
                    )
                    raise Exception(record)

            else:
                # add to DL queue
                ignoredRecordCount += 1
                print("throw to DLQ")
                raise Exception(record)

    session.end_session()

    # return response code to Lambda and log on CloudWatch
    if count == len(event["Records"]):
        print("Successfully processed %s records." % str(len(event["Records"])))
        return {"statusCode": 200, "body": json.dumps("OK")}
    else:
        print(
            "Processed only ", str(count), " records on %s" % str(len(event["Records"]))
        )
        return {"statusCode": 500, "body": json.dumps("ERROR")}
