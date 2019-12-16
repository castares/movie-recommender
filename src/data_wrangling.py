import numpy as np
import pandas as pd
import time
import re
import ast

#Dask
import dask.array as da
import dask.dataframe as dd
from dask_ml import preprocessing
from dask_ml.metrics import euclidean_distances
from dask_ml.cluster import KMeans 
from dask_ml.cluster import SpectralClustering


def processMetadata():
#This function makes some fixes on the file metadata_movies.csv to make easier to extract features from it. 

    #Loading the movies_metadata and convert "id" column to "int32".
    metadata = pd.read_csv('../input/movies_metadata.csv', low_memory=False, dtype={'id':'str'})
    metadata['id'] = metadata['id'].apply(lambda x: re.sub(r'\d+\-\d+\-\d+', "0", x))
    metadata['id'] = metadata['id'].astype("int32")
    #Transform the column "genres" from Metadata into an array with only the names of the genres.
    def getGenres(row):
        #Returns the values of key "name" from the genres dictionary.
        genres = [dct['name'] for dct in row]
        return genres
    metadata['genres'] = metadata['genres'].apply(lambda x: getGenres(ast.literal_eval(x)))
    #Loading the id links between metadata and ratings and merging it on metadata.
    links = pd.read_csv('../input/links.csv', index_col='movieId')
    metadata = metadata.merge(links['tmdbId'], how='left', left_on='id', right_on=links.index)
    metadata.to_csv("../input/movies_metadata_fixed.csv", index=False)
    return metadata

def genresDummies(metadata):
#This function receives a DataFrame with two columns (id and genres) and returns a new dataframe
# with dummy columns for all genres.
    genres = metadata[['id', 'genres']]
    genres_dummies = genres.join(genres['genres'].str.join('|').str.get_dummies())
    genres_dummies.drop("genres", axis=1, inplace=True)
    genres_dummies.set_index('id')
    genres_dummies.to_csv("../input/genres_dummies.csv", index=False)
    return genres_dummies

def filterbyRatingsAmount(ratings_ddf, min_rt, max_rt):
    #Receives a Dask Dataframe, a minimum and a maximum number of ratings per user.
    #Returns a Dask Dataframe adding a column with ratings per user to every entrance, and filtering the users outside the given range of ratings. 
    g_userId = ratings_ddf.groupby('userId')
    user_ratings = g_userId['movieId'].count().compute()
    user_ratings = user_ratings.reset_index()
    ratings_ddf = ratings_ddf.merge(user_ratings, left_on='userId', right_on='userId').compute()
    ratings_ddf = ratings_ddf.loc[ratings_ddf['movieId_y'] > min_rt]
    ratings_ddf = ratings_ddf.loc[ratings_ddf['movieId_y'] < max_rt]
    return ratings

def addUserFeatures(ratings_ddf):
    #Receives a Dask Dataframe and returns the same dataframe 
    #with 2 extra columns: rt_count for the number of ratings per users, avg_rt_user for the average rating of the user.
    g_userId = ratings_ddf.groupby('userId')
    user_ratings_count = g_userId['movieId'].count().reset_index().compute()
    ratings_2 = dd.merge(ratings_ddf, user_ratings_count, on='userId')
    user_ratings_mean = g_userId['rating'].mean().reset_index().compute()
    ratings_3 = dd.merge(ratings_2, user_ratings_mean, on='userId')
    ratings_3.compute()
    ratings_3.rename(columns={"movieId_x":"movieId", "rating_x": "rating", "movieId_y": "rt_count", "rating_y": "avg_rt_user"}, inplace=True)
    return ratings_3    


def main():
    genresDummies(processMetadata()) #Create genres_dummies.csv
    ratings = dd.read_csv('../input/ratings.csv') #ratings = dd.read_csv('/content/drive/My Drive/movie-recommender-input/ratings.csv')
    ratings.compute()
    ratings = addMoviesCount(ratings)



if __name__ == "__main__":
    main()