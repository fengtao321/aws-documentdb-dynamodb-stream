from dynamodb_json import json_util as json
import gzip
import os
from common import convert_new_image, insert_many


# miss the part to fetch the file from s3 bucket
def extract_gz():
    docs = []
    for file in os.listdir("./files"):
        if file.endswith(".gz"):
            file_in = gzip.open("./files/" + file, "rb")
            docs.append(file_in)
    return docs


# Defining main function
def main():
    requests = []
    for file in extract_gz():
        # gzipfile = gzip.GzipFile(fileobj=file)
        for line in file:
            new_image_conv = convert_new_image(line.decode())
            requests.append(new_image_conv)
    insert_many(requests)


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
