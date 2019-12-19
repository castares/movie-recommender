from bottle import post, get, request, response, run, Bottle
import re
import os
import pandas as pd

# Local Files
import src.mongodb_database as mdb
import src.features_engineering as fte


def buildDataframe(userId):
    user, to_watch = mdb.getMoviestoWatch(userId)
    df = pd.DataFrame.from_dict(to_watch)
    df = df.join(pd.get_dummies(df['genres'].apply(pd.Series)))
    df.drop(columns=['_id', 'genres'], inplace=True)
    df['user_avg_rating'] = user[0]['user_avg_rating']
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    return df[cols]


@get("/")
def home():
    return 'Movie Day'


@get("/user/<userId>")
def moviesRecommendation(userId):
    df = buildDataframe(userId)
    return None


def main():
    print(buildDataframe(49))

    # port = int(os.getenv('PORT', 8080))
    # host = os.getenv('IP', '0.0.0.0')
    # run(host=host, port=port, debug=True)


if __name__ == '__main__':
    main()
