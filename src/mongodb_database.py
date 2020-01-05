import os
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.json_util import loads, dumps
import dask.dataframe as dd
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
metadata = db['movies_metadata']


def getOverview(movieid):
    response = list(metadata.find({"id": movieid}, {"overview": 1}))
    print(response)
    if type(response) == str:
        return []
    else:
        return response[0]['overview']


def addDocument(collection, document):
    # Standard function to insert documents on MongoDB
    return collection.insert_one(document)


def addUsersbulk(ratings, userid, collection=users):
    user = ratings.loc[ratings['userId'] == userid]
    user_rt_mean = float(user['user_rt_mean'].max())
    movieids = [e for e in user['movieId'].compute()]
    new_user = {
        'userId': int(userid),
        'user_rt_mean': user_rt_mean,
        'movies_rated': movieids
    }
    addDocument(collection, new_user)
    print(f'user {userid} added to collection {collection}')


def addMoviesBulk(ratings, users_genres, movieid, collection=movies):
    movie = ratings.loc[ratings['movieId'] == movieid].head(1)
    clusters = list(
        ratings['cluster'].loc[ratings['movieId'] == movieid].unique().compute())
    movie_avg_rating = float(movie['movie_rt_mean'])
    popularity = int(movie['popularity'])
    genres_list = list(users_genres.columns)
    genres = {genre: int(movie[genre]) for genre in genres_list}
    weekdays = {str(weekday): int(movie[f'weekday_{weekday}'])
                for weekday in range(0, 7)}
    new_movie = {
        'movieId': int(movieid),
        'movie_avg_rating': float(movie_avg_rating),
        'popularity': int(popularity),
        'clusters': clusters,
        'genres': genres,
        'weekdays': weekdays
    }
    addDocument(collection, new_movie)

# TODO: The $nin on movieId doesn't work.


def getMoviestoWatch(userId):
    user = list(users.find({'userId': userId}))
    watched = user[0]['movies_rated']
    to_watch = list(movies.find(
        {'movieId': {'$nin': watched}}))
    return user, to_watch


def getMovieNames(movieIds_list):
    return metadata.find({'id': {'$in': movieIds_list}}, {'id': 1, 'original_title': 1, 'overview': 1})


def main():
    addDocument(movies, {'test': 1})


if __name__ == "__main__":
    main()
