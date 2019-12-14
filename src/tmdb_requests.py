import tmdbsimple as tmdb
from dotenv import load_dotenv
import requests
import os

load_dotenv()

api_key = os.getenv("TMDB_API_KEY")

