import numpy as np
import pandas as pd
import time
from IPython.core.display import display, HTML

# Dask
import dask.array as da
import dask.dataframe as dd
from dask_ml import preprocessing
from dask_ml.metrics import euclidean_distances
from dask_ml.cluster import KMeans
from dask_ml.cluster import SpectralClustering

from joblib import dump, load

# Sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from sklearn import svm, linear_model, tree
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor


def addUserFeatures(ratings_ddf):
    g_userId = ratings_ddf.groupby('userId')
    user_ratings_count = g_userId['movieId'].count().reset_index()
    ratings_2 = dd.merge(ratings_ddf, user_ratings_count, on='userId')
    user_ratings_mean = g_userId['rating'].mean().reset_index()
    ratings_3 = dd.merge(ratings_2, user_ratings_mean, on='userId')
    ratings_3 = ratings_3.rename(columns={
        "movieId_x": "movieId",
        "rating_x": "rating",
        "movieId_y": "user_rt_count",
        "rating_y": "user_rt_mean"
    })
    return ratings_3


def addMoviesFeatures(ratings_ddf):
    g_movieId = ratings_ddf.groupby('movieId')
    movie_ratings_mean = g_movieId['rating'].mean().reset_index()
    ratings_4 = dd.merge(ratings_ddf, movie_ratings_mean, on='movieId')
    movie_ratings_number = g_movieId['userId'].count().reset_index()
    ratings_5 = dd.merge(ratings_4, movie_ratings_number, on='movieId')
    ratings_5 = ratings_5.rename(columns={
        'userId_x': 'userId',
        'rating_x': 'GT',
        'rating_y': 'movie_rt_mean',
        'userId_y': 'popularity'
    })
    return ratings_5


def filterbyRatingsAmount(ratings_ddf, min_rt, max_rt):
    # Receives a Dask Dataframe, a minimum and a maximum number of ratings per user.
    # Returns a Dask Dataframe adding a column with ratings per user to every entrance, and filtering the users outside the given range of ratings.
    ratings_ddf = ratings_ddf.loc[ratings_ddf['user_rt_count'] > min_rt]
    ratings_ddf = ratings_ddf.loc[ratings_ddf['user_rt_count'] < max_rt]
    return ratings_ddf


def addGenresDummies(ratings_ddf, genres_dummies):
    return dd.merge(ratings_ddf, genres_dummies, left_on='movieId', right_on='id')


def addWeekdayColumns(ratings_ddf):
    ratings_ddf['weekday'] = ratings_ddf['timestamp'].apply(
        lambda x: pd.Timestamp(x, unit='s').dayofweek, meta='category')
    ratings_ddf = ratings_ddf.categorize(columns='weekday')
    weekdays = dd.get_dummies(ratings_ddf['weekday'], prefix='weekday')
    ratings_ddf['weekday'] = ratings_ddf['weekday'].astype('int64')
    return dd.concat([ratings_ddf, weekdays], axis=1, join='inner')


def ratingsNormalizer(ratings_ddf):
    # Normalize the data between 0 and 1
    scaler = preprocessing.MinMaxScaler()
    scaler.fit(ratings_ddf[["user_rt_mean", "movie_rt_mean"]])
    ratings_normalized = scaler.transform(
        ratings_ddf[["user_rt_mean", "movie_rt_mean"]])
    ratings_ddf[["user_rt_mean", "movie_rt_mean"]] = ratings_normalized
    return ratings_ddf


def popularityNormalizer(ratings_ddf):
    # Normalize the data between 0 and 1
    scaler = preprocessing.MinMaxScaler()
    scaler.fit(
        ratings_ddf[["popularity"]])
    popularity_normalized = scaler.transform(
        ratings_ddf[["popularity"]])
    ratings_ddf[["popularity"]] = popularity_normalized
    return ratings_ddf


def main():
    ratings = dd.read_csv('../input/ratings_small.csv')
    genres_dummies = pd.read_csv('../input/genres_dummies.csv')
    ratings = (ratings.pipe(addUserFeatures)
               .pipe(addMoviesFeatures)
               .pipe(filterbyRatingsAmount, min_rt=100, max_rt=500)
               .pipe(addWeekdayColumns)
               .pipe(addGenresDummies, genres_dummies=genres_dummies))
    print(ratings)
    # users_genres = dropZeroColumns(
    #     userGenresMatrix(ratings, genres_dummies).compute())


if __name__ == "__main__":
    main()
