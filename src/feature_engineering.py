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

def main():
    #ratings = pd.read_csv("../input/ratings_small.csv")
    #metadata['keywords'] = metadata['overview'].apply(lambda x: processOverview(str(x)))


    


if __name__ == "__main__":
    main()