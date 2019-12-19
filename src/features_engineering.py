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


def ratingsNormalizer(ratings_ddf):
    # Normalize the data between 0 and 1
    scaler = preprocessing.MinMaxScaler()
    scaler.fit(ratings_ddf[["mean_rt_user", "mean_rt_movie", "popularity"]])
    ratings_normalized = scaler.transform(
        ratings_ddf[["mean_rt_user", "mean_rt_user", "popularity"]])
    ratings_ddf[['mean_rt_user', 'mean_rt_user',
                 'popularity']] = ratings_normalized
    return ratings_ddf


def dropZeroColumns(df):
    # Remove columns with value max = 0 from a given Pandas DataFrame.
    to_drop = [e for e in df.columns if df[e].max() == 0]
    df = df.drop(columns=to_drop)
    return df


def defineXy(ratings_df):
    ratings_ddf = dropZeroColumns(ratings_df)
    to_X = [e for e in ratings_df.columns if ratings_df[e].max() <= 1]
    X = ratings_df[to_X]
    y = ratings_df['rating']
    return X, y


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
    ####
    filt_ratings = filterbyRatingsAmount(ratings, 100, 500)
    genres_dummies = dd.read_csv(
        '/content/drive/My Drive/movie-recommender-input/genres_dummies.csv')
    sparseMatrix(ratings, genres_dummies.compute())

    # ratings = pd.read_csv("../input/ratings_small.csv")
    # metadata['keywords'] = metadata['overview'].apply(lambda x: processOverview(str(x)))


if __name__ == "__main__":
    main()
