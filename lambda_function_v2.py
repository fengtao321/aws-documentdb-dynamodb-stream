import pymongo
import json
import os
import urllib
import datetime
from dynamodb_json import json_util as json

from package.pymongo.errors import BulkWriteError
from package.pymongo.operations import DeleteOne, InsertOne, ReplaceOne

# question about multiple lambda

# define mongodb url, username and password
# miss the part to get password from secret manage
urlDb = (
    "mongodb://pc2admin:"
    + urllib.parse.quote(
        "na&41d1CPC)7toaXEp5<+-l6ZH]!MO]Z3[4>f<4jw{x]D2QP%GNY9e]K+hcrq}(2IFLXfv&]S:D4-f4zswn:HF+e{wxo=%($N1I"
    )
    + os.environ.get("ATLAS_URI")
)
database = os.environ["ATLAS_TABLE"]
table = os.environ["ATLAS_COLLECTION"]
batch_size = int(os.environ["BATCH_SIZE"])

# define dynamodb hash key and sort key
DB_HASH_KEY = os.environ.get("DB_HASH_KEY")
DB_SORT_KEY = os.environ.get("DB_SORT_KEY")

# connect to mongodb, he default value of connect is changed to False when running in a Function-as-a-service environment.
mongo_client = pymongo.MongoClient(urlDb, connect=False)
database = mongo_client[database]
collection = database[table]

def batch_write(requests):
    try:
        response = collection.bulk_write(requests)
        result = response.bulk_api_result
    
        if len(result.writeConcernErrors) > 0:
            print(result.bulk_api_result)
            raise Exception(result.bulk_api_result)
        
        if len(result.writeErrors) > 0:
            print(result.writeErrors)
            raise Exception(result.writeErrors)
        
        return result.nModified + result.nRemoved + result.upserted + result.nInserted
    
    except BulkWriteError as bwe:
        print(bwe)
        raise Exception(bwe)
    
def convert_new_image(image):

def convert_old_image(image):
    conv = json.loads(image)
    

def lambda_handler(event, context):

    # read env variables for mongodb connection
    count = 0
    ignoredRecordCount = 0
    request = []
    
    for record in event["Records"]:
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            new_image_conv = convert_new_image(record["dynamodb"]["NewImage"])
            request.append(ReplaceOne({"_id": new_image_conv["_id"]},
                        {"$set": new_image_conv},upsert = True))
        elif record["eventName"] == "REMOVE":            
            id = convert_old_image(record["dynamodb"]["OldImage"])
            request.append(DeleteOne({"_id": id}))
        else:
            raise Exception(record)
        
        
        if len(request)==batch_size:
            count+=batch_write(request)
            request = []
    
    if len(request)>0:
        count+=batch_write(request)
            
    # return response code to Lambda and log on CloudWatch    
    if count == len(event["Records"]):
        print("Successfully processed %s records." % str(len(event["Records"])))
        return {"statusCode": 200, "body": json.dumps("OK")}
    else:
        print(
            "Processed only ", str(count), " records on %s" % str(len(event["Records"]))
        )
        return {"statusCode": 500, "body": json.dumps("ERROR")}
  
        

    with mongo_client.start_session() as session:

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
                    collection.update_one(
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
                    result = collection.delete_one(
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
