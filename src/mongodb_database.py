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
client = MongoClient()
# define database
db = client['movie_recommender']
# define collections
users = db['users']
movies = db['movies']


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


def addUsersbulk(ratings, collection=users):
    ratings_grouped = ratings.groupby(['userId'])
    for userid, _ in ratings_grouped:
        user_avg_rating = ratings_grouped.get_group(
            userid)['mean_rt_user'].max()
        movieids = [e for e in ratings_grouped.get_group(userid)['movieId']]
        new_user = {
            'userId': int(userid),
            'user_avg_rating': user_avg_rating,
            'movies_rated': movieids
        }
        addDocument(collection, new_user)
        print(f'user {userid} added to collection {collection}')


def addMoviesBulk(ratings, genres_list, collection=movies):
    movies_grouped = ratings.groupby(['movieId'])
    for movieid, _ in movies_grouped:
        movie_avg_rating = movies_grouped.get_group(
            movieid)['mean_rt_movie'].max()
        popularity = movies_grouped.get_group(movieid)['popularity'].max()
        cluster = movies_grouped.get_group(movieid)['clusters'].max()
        genres = [{genre: int(movies_grouped.get_group(
            movieid)[genre].max())} for genre in genres_list]
        new_movie = {
            'movieId': int(movieid),
            'movie_avg_rating': float(movie_avg_rating),
            'popularity': int(popularity),
            'cluster': int(cluster),
            'genres': genres,
        }
        addDocument(collection, new_movie)
        print(f'movie {movieid} added to collection {collection}')


def buildDataframe(userid):
    user = users.find({"userId": userid})
    movies = movies.find({"movieId": {"$in": user[0]['movies_rated']}})
    df = pd.DataFrame({
        ''
    })


def main():
    # ratings = dd.read_csv('../output/ratings-0/ratings_0-*.csv')
    # ratings = ratings.rename(
    #     columns={'mean_rt_user': 'mean_rt_movie', 'avg_rt_user': 'mean_rt_user', })
    # ratings = ratings.compute()
    # addUsersbulk(ratings)
    # genres_list = ['Action', 'Adventure', 'Animation',
                   'Aniplex', 'BROSTA TV', 'Carousel Productions', 'Comedy', 'Crime',
                   'Documentary', 'Drama', 'Family', 'Fantasy', 'Foreign', 'GoHands',
                   'History', 'Horror', 'Mardock Scramble Production Committee', 'Music',
                   'Mystery', 'Odyssey Media', 'Pulser Productions', 'Rogue State',
                   'Romance', 'Science Fiction', 'Sentai Filmworks', 'TV Movie',
                   'Telescene Film Group Productions', 'The Cartel', 'Thriller',
                   'Vision View Entertainment', 'War', 'Western']
    # addMoviesBulk(ratings, genres_list)

    # collist = db.list_collection_names()
    # # Create New Collections
    # new_collections = ["movies_metadata", "keywords", "links", "credits"]
    # for coll in new_collections:
    #     if coll not in collist:
    #         db.create_collection(coll)
    #         print(f"collection {coll} created")
    #     else:
    #         print(f"Collection {coll} already exists")
if __name__ == "__main__":
    main()
