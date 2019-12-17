import numpy as np
import pandas as pd
import time

# NLTK
import nltk
from nltk import word_tokenize, pos_tag

# Dask
import dask.dataframe as dd

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


def addUserFeatures(ratings_ddf):
    g_userId = ratings_ddf.groupby('userId')
    user_ratings_count = g_userId['movieId'].count().reset_index().compute()
    ratings_2 = dd.merge(ratings_ddf, user_ratings_count, on='userId')
    user_ratings_mean = g_userId['rating'].mean().reset_index().compute()
    ratings_3 = dd.merge(ratings_2, user_ratings_mean, on='userId')
    ratings_3 = ratings_3.rename(columns={
                                 "movieId_x": "movieId", "rating_x": "rating", "movieId_y": "rt_count", "rating_y": "avg_rt_user"})
    return ratings_3


def addMoviesFeatures(ratings_ddf):
    g_movieId = ratings_ddf.groupby('movieId')
    movie_ratings_mean = g_movieId['rating'].mean().reset_index().compute()
    ratings_4 = dd.merge(ratings_ddf, movie_ratings_mean, on='movieId')
    movie_ratings_number = g_movieId['userId'].count().reset_index().compute()
    ratings_5 = dd.merge(ratings_4, movie_ratings_number, on='movieId')
    ratings_5 = ratings_5.rename(columns={
        'rating_x_x': 'GT', 'rating_y': 'mean_rt_user', 'rating_x_y': 'mean_rt_movie', 'userId_y': 'popularity'
    })
    return ratings_5


def filterbyRatingsAmount(ratings_ddf, min_rt, max_rt):
    # Receives a Dask Dataframe, a minimum and a maximum number of ratings per user.
    # Returns a Dask Dataframe adding a column with ratings per user to every entrance, and filtering the users outside the given range of ratings.
    ratings_ddf = ratings_ddf.loc[ratings_ddf['rt_count'] > min_rt]
    ratings_ddf = ratings_ddf.loc[ratings_ddf['rt_count'] < max_rt]
    return ratings_ddf


def addGenresDummies(ratings_ddf, genres_dummies):
    return dd.merge(ratings_ddf, genres_dummies, left_on='movieId', right_on='id')


def addWeekdayColumn(ratings_ddf):
    ratings_ddf['weekday'] = ratings_ddf['timestamp'].apply(
        lambda x: pd.Timestamp(x).dayofweek, meta=int)
    return ratings_ddf


def main():
    ratings = dd.read_csv('../input/ratings.csv')
    # ratings = dd.read_csv('/content/drive/My Drive/movie-recommender-input/ratings.csv')
    ratings = addUserFeatures(ratings)
    ratings = addMoviesFeatures(ratings)
    ratings = filterbyRatingsAmount(ratings, 100, 500)
    genres_dummies = dd.read_csv(
        '/content/drive/My Drive/movie-recommender-input/genres_dummies.csv')
    genres_dummies.compute()
    ratings = addGenresDummies(ratings_ddf, genres_dummies)
    # drop 0 value columns
    ratings = addWeekdayColumn(ratings_ddf)
    ratings.compute()

    ratings =
    filt_ratings = filterbyRatingsAmount(ratings, 100, 500)
    genres_dummies = dd.read_csv(
        '/content/drive/My Drive/movie-recommender-input/genres_dummies.csv')
    sparseMatrix(ratings, genres_dummies.compute())

    #ratings = pd.read_csv("../input/ratings_small.csv")
    #metadata['keywords'] = metadata['overview'].apply(lambda x: processOverview(str(x)))


if __name__ == "__main__":
    main()
