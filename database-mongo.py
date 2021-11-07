import json
from pymongo import MongoClient

# reference: https://stackoverflow.com/questions/49510049/how-to-import-json-file-to-mongodb-using-python

client = MongoClient('localhost', 27017)
client.list_database_names()

db = client['municipalities']

collection = db['municipalities']

print(db.list_collection_names())

with open('brazil-geometries.json') as f:
    file_data = json.load(f)

# Our document is too big to be stored in a single collection, though
collection.insert_one(file_data)

client.close()