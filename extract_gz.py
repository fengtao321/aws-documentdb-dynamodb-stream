import gzip
import shutil
import os
import json

for file in os.listdir("./files"):
    if file.endswith(".gz"):
        file_in = gzip.open("./files/" + file, "rb")
        result = file_in.read()
        result = result.decode()
        json_file_handle = open("./files/extract/" + file[:-3], "w")
        
        json_file_handle.write(result)
        json_file_handle.close()

i = 0