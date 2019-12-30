import numpy as np
import pandas as pd
import time
import re
import ast


def getGenres(row):
    # Returns the values of key "name" from the genres dictionary.
    genres = [dct['name'] for dct in row]
    metadata['genres'] = metadata['genres'].apply(
        lambda x: getGenres(ast.literal_eval(x)))
    # Loading the id links between metadata and ratings and merging it on metadata.
    links = pd.read_csv('../input/links.csv', index_col='movieId')
    metadata = metadata.merge(
        links['tmdbId'], how='left', left_on='id', right_on=links.index)
    metadata.to_csv("../input/movies_metadata_fixed.csv", index=False)
    return metadata


def processMetadata():
    # This function makes some fixes on the file metadata_movies.csv to make easier to extract features from it.
    # Loading the movies_metadata and convert "id" column to "int32".
    metadata = pd.read_csv('../input/movies_metadata.csv',
                           low_memory=False, dtype={'id': 'str'})
    metadata['id'] = metadata['id'].apply(
        lambda x: re.sub(r'\d+\-\d+\-\d+', "0", x))
    metadata['id'] = metadata['id'].astype("int32")
    # Transform the column "genres" from Metadata into an array with only the names of the genres.


def genresDummies(metadata):
    # This function receives a DataFrame with two columns (id and genres) and returns a new dataframe
    # with dummy columns for all genres.
    genres = metadata[['id', 'genres']]
    genres_dummies = genres.join(
        genres['genres'].str.join('|').str.get_dummies())
    genres_dummies.drop("genres", axis=1, inplace=True)
    to_drop = [e for e in genres_dummies.columns if genres_dummies[e].max() == 0]
    genres_dummies.drop(columns=to_drop, inplace=True)
    genres_dummies.set_index('id')
    genres_dummies.to_csv("../input/genres_dummies.csv", index=False)
    return genres_dummies

# TODO: implement the dropzerocolumns on the creation of the genres_dummies doc.
# genres_dummies = dropZeroColumns(genres_dummies.compute())


def main():
    genresDummies(processMetadata())  # Create genres_dummies.csv


if __name__ == "__main__":
    main()
