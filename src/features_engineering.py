import numpy as np
import pandas as pd
import time

#NLTK
import nltk
from nltk import word_tokenize, pos_tag

#Dask
import dask.array as da
import dask.dataframe as dd
from dask_ml import preprocessing
from dask_ml.metrics import euclidean_distances
from dask_ml.cluster import KMeans 
from dask_ml.cluster import SpectralClustering

# def processOverview(overview):
#     valid_tags = ["JJ","JJR","JJS","NN","NNS"]
#     tokens = [item for item, tag in pos_tag(word_tokenize(overview)) if tag in valid_tags]
#     return tokens


# def getTokenlist(lst):
#     tokens_list = []
#     for movieid in lst:
#         tokens_list.append(",".join(processOverview(movieid)))
#         print(f"movie: {movieid} processed")
#     print(len(set(tokens_list)))

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

def addMoviesFeatures(ratings_ddf):
    g_movieid = ratings.groupby('movieId')
    movie_ratings_mean = g_movieid['rating'].mean().reset_index().compute()
    ratings_4 = dd.merge(ratings_ddf, movie_ratings_mean, on='movieId')
    movie_ratings_number = g_movieid.userId.count().reset_index().compute() 
    ratings_5 = dd.merge(ratings_4, movie_ratings_number, on='movieId')
    ratings_5.compute()
    ratings_5 = ratings.rename(columns={'rating_x_x':'GT', 'rating_y':'mean_rt_user', 'rating_x_y':'mean_rt_movie', 'userId_y':'popularity'})
    return ratings_5

def filterbyRatingsAmount(ratings_ddf, min_rt, max_rt):
    #Receives a Dask Dataframe, a minimum and a maximum number of ratings per user.
    #Returns a Dask Dataframe adding a column with ratings per user to every entrance, and filtering the users outside the given range of ratings. 
    ratings_ddf = ratings_ddf.loc[ratings_ddf['movieId_y'] > min_rt]
    ratings_ddf = ratings_ddf.loc[ratings_ddf['movieId_y'] < max_rt]
    return ratings_ddf

def sparseMatrix(ratings_ddf, genres_dummies):
    genres_dummies = genres_dummies.set_index('id')
    users_genres = dd.merge(ratings_ddf, genres_dummies, left_on='movieId_x', right_on=genres_dummies.index)
    g_userid = users_genres.groupby('userId')
    return g_userid[genres_dummies.columns].sum()


def main():
    ratings = dd.read_csv('../input/ratings.csv') #ratings = dd.read_csv('/content/drive/My Drive/movie-recommender-input/ratings.csv')
    ratings = addMoviesFeatures(addUserFeatures(ratings.compute()))
    filt_ratings = filterbyRatingsAmount(ratings, 100, 500)
    genres_dummies = dd.read_csv('/content/drive/My Drive/movie-recommender-input/genres_dummies.csv')
    sparseMatrix(ratings, genres_dummies.compute())





    #ratings = pd.read_csv("../input/ratings_small.csv")
    #metadata['keywords'] = metadata['overview'].apply(lambda x: processOverview(str(x)))


    


if __name__ == "__main__":
    main()