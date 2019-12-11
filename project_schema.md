#Movie Recommender

- Names: MovieDay, muvi.

- Purpose: Create a recommendation engine able to pick the best movies for a given user on a given timestamp.

- Target Outcome: Create a recommendation engine able to pick the best movies for a given user on a given hour range.

- Tools:
    - Main Dataset: [The Movies Dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset#movies_metadata.csv). *(Metadata on over 45,000 movies. 26 million ratings from over 270,000 users.)*
    - Databases:
        - Postgres or MySQL for the movies metadata.
        - Graphite or TimescaleDB for the ratings data.
    - Libraries: 
        - Surprise 