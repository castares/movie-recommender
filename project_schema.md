#Movie Recommender

- Names: MovieDay, muvi.

- Purpose: Create a recommendation engine able to pick the best movies for a given user on a given timestamp.

- Target Outcome: Create a recommendation engine able to pick the best movies for a given user on a given hour range.

- Tools:
    - Main Dataset: [The Movies Dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset#movies_metadata.csv). *(Metadata on over 45,000 movies. 26 million ratings from over 270,000 users.)*
    - Databases:
        - MySQL for the movies metadata.
        - Graphite or TimescaleDB for the ratings data.
    - Libraries: 
        - Surprise 


- Data Analysis & Feature engineering:
    - ratings.csv:
        - Remove outliers (users with <50 or >500 ratings?)
        - Create weekday column
        - Create hour bins column (every 3-4 hours?)


- Users Clustering: 
    - Keywords of the seen movies: NLTK of the movies sinopsis
    - Keywords file?