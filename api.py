#! /usr/bin/python3

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


def buildDataframe(userId, weekday):
    user, to_watch = mdb.getMoviestoWatch(userId)
    df = pd.DataFrame.from_dict(to_watch)
    df = df.join(pd.get_dummies(df['genres'].apply(pd.Series)))
    for e in range(0, 7):
        df[f'weekday_{e}'] = 0
    df[f'weekday_{weekday}'] = 100
    df['user_rt_mean'] = user[0]['user_rt_mean']
    return df[['movieId', 'user_rt_mean', 'movie_rt_mean', 'popularity', 'weekday_0', 'weekday_1',
               'weekday_2', 'weekday_3', 'weekday_4', 'weekday_5', 'weekday_6',
               'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary',
               'Drama', 'Family', 'Fantasy', 'Foreign', 'History', 'Horror', 'Music',
               'Mystery', 'Romance', 'Science Fiction', 'TV Movie', 'Thriller', 'War',
               'Western']]


def recommenderWeekday(userId, weekday):
    df = buildDataframe(userId, weekday)
    X = df.drop(columns='movieId')
    prediction = gbr.predict(X)
    df['prediction'] = prediction
    X['movieId'] = df['movieId']
    df = df.sort_values('prediction', ascending=False)
    metadata = list(mdb.getMovieNames(list(df['movieId'][0:10])))
    results = []
    for e in metadata:
        result = {
            "id": e['id'],
            'name': e['original_title'],
            'predicted rating': round(float(df['prediction'].loc[df['movieId'] == e['id']]), 1)
        }
        results.append(result)
    return results


with open("output/models/gbrdefaultpickle_file.joblib", 'rb') as gbrpickle:
    gbr = load(gbrpickle)


@get("/")
def home():
    return 'Movie Day'


@get("/user/<userId>")
def moviesRecommendation(userId):
    # try:
    #     int(userId)
    #     int(weekday)
    # except:
    #     raise ValueError(f'{userId} and {weekday} must be integers.')
    return json.dumps(recommenderWeekday(int(userId), int(weekday)))


def main():
    port = int(os.getenv('PORT', 8080))
    host = os.getenv('IP', '0.0.0.0')
    run(host=host, port=port, debug=True)


if __name__ == '__main__':
    main()
