from bottle import post, get, request, response, run, Bottle
import re
import os
import pandas as pd
import json

# Local Files
import src.mongodb_database as mdb
import src.features_engineering as fte

from joblib import dump, load
from sklearn.ensemble import GradientBoostingRegressor
from dask_ml import preprocessing


def buildDataframe(userId):
    user, to_watch = mdb.getMoviestoWatch(userId)
    df = pd.DataFrame.from_dict(to_watch)
    df = df.join(pd.get_dummies(df['genres'].apply(pd.Series)))
    df.drop(columns=['_id', 'genres'], inplace=True)
    df['user_avg_rating'] = user[0]['user_avg_rating']
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    return df[cols]


def popularityNormalizer(ratings_ddf):
    # Normalize the data between 0 and 1
    scaler = preprocessing.MinMaxScaler()
    scaler.fit(
        ratings_ddf[["popularity"]])
    ratings_normalized = scaler.transform(
        ratings_ddf[["popularity"]])
    ratings_ddf[["popularity"]] = ratings_normalized
    return ratings_ddf


def testrecommender(userId):
    df = buildDataframe(userId)
    df = popularityNormalizer(df)
    df = fte.dropZeroColumns(df)
    to_X = [e for e in df.columns if df[e].max() <= 1]
    X = df[to_X]
    # X.drop(columns='movieId', inplace=True)
    prediction = gbr.predict(X)
    X['prediction'] = prediction
    X['movieId'] = df['movieId']
    X = X.sort_values('prediction', ascending=False)
    metadata = list(mdb.getMovieNames(list(X['movieId'][0:10])))
    results = []
    for e in metadata:
        result = {
            "id": e['id'],
            'name': e['original_title'],
            'predicted rating': float(X['prediction'].loc[X['movieId'] == e['id']])
        }
        results.append(result)
    return results


with open("output/gbrpickle_file.joblib", 'rb') as gbrpickle:
    gbr = load(gbrpickle)


@get("/")
def home():
    return 'Movie Day'


@get("/user/<userId>")
def moviesRecommendation(userId):
    return json.dumps(testrecommender(int(userId)))


def main():
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('IP', '0.0.0.0')
    run(host=host, port=port, debug=True)


if __name__ == '__main__':
    main()
