
import pymongo

from pymongo import MongoClient

# Making a connection with MongoClient
client = MongoClient('localhost', 27017)

# Getting a Database
db = client['test-database']

# Getting a Collection
collection = db['test-collection']

# Documents
import datetime
post = {
			"author": "Mike",
			"text": "My first blog post!",
			"tags": [ "mongodb", "python", "pymongo" ],
			"date": datetime.datetime.utcnow(),
		}

# Inserting a Document
posts = db.posts
post_id = posts.insert_one(post).inserted_id
print("post_id:", post_id, "posts:", posts)

import pprint
#
pprint.pprint(posts.find_one())

pprint.pprint(posts.find_one({ "_id": post_id, }))

