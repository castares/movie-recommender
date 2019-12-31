import numpy as np
import pandas as pd
import re
import ast


def metadataId(metadata):
    # Remove errors on the field "id" of the movies_metadata file.
    metadata['id'] = metadata['id'].apply(
        lambda x: re.sub(r'\d+\-\d+\-\d+', "0", x))
    metadata['id'] = metadata['id'].astype("int32")
    return metadata


def metadataGenres(metadata):
    metadata['genres'] = metadata['genres'].apply(
        lambda x: [dct['name'] for dct in ast.literal_eval(x)])
    return metadata


def metadataLinks(metadata, links):
    metadata = metadata.merge(
        links['tmdbId'], how="left", left_on="id", right_index=True)
    return metadata


def genresDummies(metadata):
    # Creares a dataframe with dummy columns for all genres.
    genres = metadata[['id', 'genres']]
    genres_dummies = genres.join(
        genres['genres'].str.join('|').str.get_dummies())
    genres_dummies.drop("genres", axis=1, inplace=True)
    to_drop = [e for e in genres_dummies.columns if genres_dummies[e].max() == 0]
    genres_dummies.drop(columns=to_drop, inplace=True)
    genres_dummies.set_index('id')
    genres_dummies.to_csv("../input/genres_dummies.csv", index=False)
    return


def main():
    metadata = pd.read_csv('../input/movies_metadata.csv',
                           low_memory=False, dtype={'id': 'str'})
    links = pd.read_csv('../input/links.csv', index_col='movieId')
    metadata = (metadata.pipe(metadataId)
                .pipe(metadataGenres)
                .pipe(metadataLinks, links=links)
                )
    genresDummies(metadata)  # new file genres_dummies.csv
    metadata.to_csv('../input/movies_metadata_fixed.csv')  # new file


if __name__ == "__main__":
    main()
