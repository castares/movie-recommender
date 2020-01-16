# Movie Time API

This API is deployed in Heroku and is available at: https://movie-time-api.herokuapp.com/

All responses are in JSON.

1. User Enpoint:


*GET* *"/user/[userid]/recommendation"*: Produces a recommendation for the users with their would-be 10 top-rated movies from a list based on their current ratings. To see in detail how this endpoint works, check [the pipeline documentation](https://github.com/castares/movie-time/blob/master/README.md)
- *Response:* `user_id`, `predictions`
    - Example: 
        {"userId": 671, "predictions": [{"id": 2086, "name": "Nick of Time", "predicted rating": 4.9}, {"id": 845, "name": "Strangers on a Train", "predicted rating": 4.9}, {"id": 8675, "name": "Orgazmo", "predicted rating": 4.9}, {"id": 26791, "name": "Brigham City", "predicted rating": 4.9}, {"id": 3112, "name": "The Night of the Hunter", "predicted rating": 4.9}, {"id": 876, "name": "Frank Herbert\'s Dune", "predicted rating": 4.9}, {"id": 8699, "name": "Anchorman: The Legend of Ron Burgundy", "predicted rating": 4.9}, {"id": 4140, "name": "Blindsight", "predicted rating": 4.9}, {"id": 59392, "name": "Contraluz", "predicted rating": 4.9}, {"id": 6163, "name": "The Hessen Affair", "predicted rating": 4.9}]}


*GET* *"/user/[userid]/rated"*: Returns all the movies rated by a given user.
- *Response:* `movieId`, `rating`
    - Example: 
        {'671': [{'movieId': 296, 'rating': 4.0},
        {'movieId': 457, 'rating': 4.0},
        {'movieId': 551, 'rating': 5.0},
        {'movieId': 588, 'rating': 4.0},
        {'movieId': 590, 'rating': 4.0},
        ....

*GET* *"/user/list/[clusterid]"*: Returns all the users assigned to the given cluster.
- *Response:* `userId`, `clusterid`, `user_rt_mean`
    - Example: 
    [{'userId': 17, 'user_rt_mean': 3.743801652892562, 'cluster': 3},
    {'userId': 48, 'user_rt_mean': 3.5146198830409356, 'cluster': 3},
    ...

*GET* *"/movie/[movieid]/metadata"*: Returns the metadata of a given movie.
- *Response:* `movieid`, `genres`, `imdb_id`, `revenue`, `runtime`
    - Example:
    [{'genres': "['Drama', 'Crime']",
  'id': 2,
  'imdb_id': 'tt0094675',
  'revenue': 0.0,
  'runtime': 69.0}]

*GET* *"/movie/list/[clusterid]"*: Returns all the movies rated by users assigned to a given cluster id.
- *Response:* `movieId` `movie_rt_mean` `popularity` `clusters` `genres`
    - Example: {'movieId': 2,
 'movie_rt_mean': 3.4018691588785046,
 'popularity': 107,
 'clusters': [1, 0, 3, 2],
 'genres': {'Action': 0,
  'Adventure': 0,
  'Animation': 0,
  'Aniplex': 0,
  'BROSTA TV': 0,
  'Carousel Productions': 0,
  'Comedy': 0,
  'Crime': 1,
  'Documentary': 0,
  'Drama': 1,
  'Family': 0,
  'Fantasy': 0,
  'Foreign': 0,
  'GoHands': 0,
  'History': 0,
  'Horror': 0,
  'Mardock Scramble Production Committee': 0,
  'Music': 0,
  'Mystery': 0,
  'Odyssey Media': 0,
  'Pulser Productions': 0,
  'Rogue State': 0,
  'Romance': 0,
  'Science Fiction': 0,
  'Sentai Filmworks': 0,
  'TV Movie': 0,
  'Telescene Film Group Productions': 0,
  'The Cartel': 0,
  'Thriller': 0,
  'Vision View Entertainment': 0,
  'War': 0,
  'Western': 0}}