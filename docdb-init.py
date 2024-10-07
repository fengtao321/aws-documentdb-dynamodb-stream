from dynamodb_json import json_util as json
import gzip
import os
from common import convert_new_image, batch_write
from pymongo import UpdateOne
import boto3

S3_BUCKET = "p2-admin-portal-tao"
S3_FOLDER = "AWSDynamoDB/01728330902696-439a1531/data/"


def extract_gz_from_local():
    print("Start to extract_gz_from_local")
    requests = []
    
    for file in os.listdir("./files"):
        if file.endswith(".gz"):
            file_in = gzip.open("./files/" + file, "rb")

            for line in file_in:
                new_image_conv = convert_new_image(line.decode())
                requests.append(
                    UpdateOne(
                        {"_id": new_image_conv["_id"]},
                        {"$set": new_image_conv},
                        upsert=True,
                    )
                )
    return requests
    
def extract_gz_from_s3():
    print("Start to extract_gz_from_s3")
    
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(S3_BUCKET)
    requests = []
    
    for obj in bucket.objects.filter(Prefix=S3_FOLDER):
        if not obj.key.endswith(".gz"):
            continue
        
        print("object key is::",  obj.key)
        with gzip.open(obj.get()["Body"]) as file_in:
            for line in file_in:
                new_image_conv = convert_new_image(line.decode())
                requests.append(
                    UpdateOne(
                        {"_id": new_image_conv["_id"]},
                        {"$set": new_image_conv},
                        upsert=True,
                    )
                )
    return requests

# Defining main function
def main():
    requests = extract_gz_from_s3() if S3_BUCKET!="" and S3_FOLDER!="" else extract_gz_from_local();
    batch_write(requests)

# Using the special variable
# __name__
if __name__ == "__main__":
    main()
