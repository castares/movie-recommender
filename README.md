 # Movie Time

Movie Time is a collaborative recommender system built on Machine Learning using the [MovieLens Dataset](https://grouplens.org/datasets/movielens/latest/) metadata from Kaggle's [The Movies Dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset). Using Scikit-Learn for both Clustering and user rating prediction. The feature extraction and Machine Learning process are written in Pandas and Dask for full scalability. The data is stored on a MongoDB Atlas and is accessible through a bottle based API deployed in Heroku.

1. The Pipeline 

(To see an example of the processs explained below, go to [pipeline.ipynb](https://github.com/castares/movie-time/blob/master/pipeline.ipynb) notebook.)

1.1. Features Extraction:
The original Dataset consisted on a list of movies ratings, including four columns: Timestamp, User Id, Movie Id and a Rating from 0 to 5. Using that information and the metadata of the movies, I have extracted up to 30 columns, including: The mean of user and movie ratings, the movie popularity (count of ratings within the Dataset), the genres of the movies (dummy columns for each available genre), and the day of the week in which the rating has been done (also dummy columns). 

1.2 Clustering:
Taking some of the features, I have created a dataframe with the users and their preferences of movie genres. To do this I summed the genres dummy columns from each movie rating and normalized the results. I passed this table to an Spectral Clustering algorithm to obtain 4 Clusters of users.

1.3 Movie Rating Prediction: 
Using the whole dataset with the extracted features, I have searched for the best algorithm to predict user ratings, using RSME as metric. I obtained the best results with the Gradient Boosting Regressor with their default parameters (the metric does not get better with tuning), getting an RSME of 0.81.

1.4 The Database:
Once the Clusters are defined, I have stored separately the data in MongoDB Atlas, splitting the data in three collections: Users, Movies & Metadata. To interact with this data, see below the [API documentation]().

2. The API


(To see full documentation of the API, go to API Documentation).


URL: https://movie-time-api.herokuapp.com/

