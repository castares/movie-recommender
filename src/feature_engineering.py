import pandas as pd
#NLTK
import nltk
from nltk import word_tokenize, pos_tag

#Local files
import mongodb_database as mdb

def processOverview(overview):
    valid_tags = ["JJ","JJR","JJS","NN","NNS"]
    tokens = [item for item, tag in pos_tag(word_tokenize(overview)) if tag in valid_tags]
    return tokens


def getTokenlist(lst):
    tokens_list = []
    for movieid in lst:
        tokens_list.append(",".join(processOverview(movieid)))
        print(f"movie: {movieid} processed")
    print(len(set(tokens_list)))

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

def main():
    #ratings = pd.read_csv("../input/ratings_small.csv")
    #metadata['keywords'] = metadata['overview'].apply(lambda x: processOverview(str(x)))


    


if __name__ == "__main__":
    main()