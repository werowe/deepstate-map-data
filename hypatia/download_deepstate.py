import datetime
import json
import requests
from pymongo.errors import BulkWriteError, OperationFailure, PyMongoError
from pymongo import MongoClient
import hashlib
from dotenv import load_dotenv
import os


url = "https://deepstatemap.live/api/history/last"

rsp = requests.get(url)

data = rsp.json()

dt = datetime.datetime.fromtimestamp(data['id'])

# Format to string
readable_time = dt.strftime('%Y-%m-%d %H:%M:%S')

print("Downloaded at UTC", readable_time)

file = "/home/werowe/Documents/deepstate/data/" + str(data['id']) + ".json"

with open(file, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=4, ensure_ascii=False)
    
load_dotenv()

# 2. Access the variables
user = os.getenv("MONGODB_USER")
pwd = os.getenv("MONGODB_PWD")
cluster = "cluster0.roydclf.mongodb.net/?appName=Cluster0"


connection_string = f"mongodb+srv://{user}:{pwd}@{cluster}"

client = MongoClient(connection_string)


db = client['geodb']
collection = db['geodata']

# use only one time
#collection.create_index("doc_hash", unique=True)

epoch_time = data['id']


dt_formatted = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y %m %d %H %M %S')
print("processing epoch {} human {}".format(epoch_time, dt_formatted))

docs = []
 

for i in data['map']['features']:
    i['epoch_time'] = epoch_time
    doc_string = json.dumps(i, sort_keys=True)
    doc_hash = hashlib.sha256(doc_string.encode('utf-8')).hexdigest()
    i['doc_hash'] = doc_hash  

    docs.append(i)

try:
    result = collection.insert_many(docs, ordered=False)
    print("Inserted", len(result.inserted_ids), "documents", dt)

except BulkWriteError as bwe:
    inserted_count = bwe.details['nInserted']
    
    print(bwe.details.get("writeErrors", []))

    print(f"\n\n\nInserted {inserted_count} new documents. (Some duplicates were skipped).", dt)
                                                

except OperationFailure as oe:
    # Handles authentication failures, permission denied, etc.
    print(f"MongoDB Operation failed: {oe}", dt)
    oe.details

except PyMongoError as e:
    # Catch-all for any other MongoDB connection or driver errors
    print(f"General MongoDB error: {e}", dt)

except Exception as e:
    # Catch-all for non-database errors (like if 'docs' variable is malformed)
    print(f"Unexpected Python error: {e}", dt)

now = datetime.datetime.now()

print("processing epoch {} human {}".format(epoch_time, dt_formatted))
# Format as YY-MM-DD HH:MM:SS
formatted_date = now.strftime("%y-%m-%d %H:%M:%S")

print("run on ", now)



print("================================================")
