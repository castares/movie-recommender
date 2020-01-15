import os
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.json_util import loads, dumps
import pandas as pd

load_dotenv()
# Connection to Atlas Cluster
mongodbUrl = os.getenv("MONGODBURL")
# define Client, Database and collections on that database
client = MongoClient(mongodbUrl)
# define database
db = client['movie-recommender']
# define collections
users = db['users']
movies = db['movies']
metadata = db['metadata']


def addDocument(collection, document):
    # Standard function to insert documents on MongoDB
    return collection.insert_one(document)


def getUser(userId):
    return list(users.find({'userId': userId}, {'_id': 0}))


def topratedMovies(userId):
    user = getUser(userId)
    return {userId: user[0]["ratings"]}


def getMoviestoWatch(userId):
    user = getUser(userId)
    watched = user[0]['movies_rated']
    to_watch = list(movies.find(
        {'clusters': {'$in': [user[0]['cluster']]}, 'movieId': {'$nin': watched}}))
    return user, to_watch


def getusersByCluster(cluster):
    return list(users.find({'cluster': cluster}, {'_id': 0, 'userId': 1, 'cluster': 1, 'user_rt_mean': 1}))


def getMovieNames(movieIds_list):
    return metadata.find({'id': {'$in': movieIds_list}}, {'id': 1, 'original_title': 1, 'overview': 1})


def getMovieMetadata(movieid):
    return list(metadata.find({'id': movieid}, {'_id': 0, 'id': 1, 'genres': 1, 'imdb_id': 1, 'revenue': 1, 'runtime': 1}))


def getmoviesbyCluster(clusterid):
    return list(movies.find({'clusters': {"$in": [clusterid]}}, {'_id': 0}))


def main():
    print(getUser(671))


if __name__ == "__main__":
    main()
