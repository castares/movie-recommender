import os
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.json_util import loads, dumps

load_dotenv()

#mongodbUrl = os.getenv("MONGODBURL")

# define Client, Database and collections on that database
client = MongoClient()
# define database
db = client['movie_recommender']
# define collections
metadata = db['movies_metadata']


def getOverview(movieid):
    response = list(metadata.find({"id":movieid},{"overview":1}))
    print(response)
    if type(response) == str:
        return []
    else: 
        return response[0]['overview']

def main():
    # collist = db.list_collection_names()
    # # Create New Collections
    # new_collections = ["movies_metadata", "keywords", "links", "credits"]
    # for coll in new_collections:
    #     if coll not in collist:
    #         db.create_collection(coll)
    #         print(f"collection {coll} created")
    #     else:
    #         print(f"Collection {coll} already exists")
    getOverview(78802)

if __name__ == "__main__":
    main()
