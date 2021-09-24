import datetime
import base64
import json
from pandas.core.frame import DataFrame

import requests
import pandas as pd

# Comment this line if you input CLEINT_ID and CLIENT_SECRET below
from secrets import CLIENT_ID, CLIENT_SECRET
import get_auth_code
import database

# Client credentials
# Client credentials are kept in a separate file secrets.py, but you can input them here instead.
# CLIENT_ID = ""
# CLIENT_SECRET = ""

# Spotify URLs
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"

# Server-side Parameters
CLIENT_SIDE_URL = "http://127.0.0.1"
PORT = "8080"
REDIRECT_URI = f"{CLIENT_SIDE_URL}:{PORT}/callback/q"

# Authentication query parameters
SCOPE = "user-read-recently-played"
STATE = ""
SHOW_DIALOG = "false"

# Authorization code
AUTH_TOKEN = None


def get_recently_played(token):
    endpoint = "https://api.spotify.com/v1/me/player/recently-played"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    params = {"limit": 50}
    r = requests.get(endpoint, headers=headers, params=params)
    if r.status_code not in range(200, 299):
        print(r.json())
        raise Exception("Could not get requested user data.")
    return r.json()


def get_artist_genres(df):
    artist_genres = []
    artist_id_list = df["artist_id"].tolist()
    artist_base_url = "https://api.spotify.com/v1/artists/"
    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
    }

    for id in artist_id_list:
        artist_url = f"{artist_base_url}{id}"
        r = requests.get(artist_url, headers=headers)
        if r.status_code not in range(200, 299):
            return []
        genres = r.json()["genres"]
        artist_genres.append(genres)
        
    df["artist_genre"] = artist_genres
    
    return df

def get_track_features(df: DataFrame) -> DataFrame:
    track_features = []
    track_list = df["song_id"].tolist()
    audio_features_base_url = "https://api.spotify.com/v1/audio-features/"
    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
    }

    for _ in track_list:
        audio_features_url = f"{audio_features_base_url}{id}"
        r = requests.get(audio_features_url, headers=headers)
        if r.status_code not in range(200, 299):
            return []
        genres = r.json()["genres"]
        track_features.append(genres)

    return track_features


def check_if_data_valid(df):
    # Check if dataframe is empty
    if df.empty:
        print("No tracks downloaded.")
        return False
    
    # Check if Primary Key values are unique
    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary Key contraint is violated.")

    # Check for null values
    if df.isnull().values.any():
        raise Exception("Null values found in the dataset.")
    
    # Check if the tracks played at date is yesterday
    now = datetime.datetime.now()
    yesterday = now - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["played_at"].tolist()
    for timestamp in timestamps:
        timestamp = datetime.datetime.strptime(timestamp[0:10], "%Y-%m-%d")
        if timestamp != yesterday:
            raise Exception("At least one of the returned songs does not come within the last 24 hours.")

    return True

def filter_by_played_at(df, days_interval=1):
    # Donwloads the tracks data only from the day before.
    # This funcionality will be improved once tha databse is created.
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(days=days_interval)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = yesterday.strftime("%Y-%m-%d")

        df["played_at_timestamp"] = df["played_at"].apply(lambda x: x[0:10])
        df = df[df["played_at_timestamp"] == yesterday]
        
        return df.drop(["played_at_timestamp"], axis=1)
        

  
if __name__ == "__main__":
    AUTH_TOKEN = get_auth_code.obtain_auth_code()
    print(f"The authorization code is: {AUTH_TOKEN}")
    token = get_auth_code.get_token()
    data = get_recently_played(token)
    
    artist_name = []
    type = []
    duration = []
    is_explicit = []
    song_name = []
    song_popularity = []
    song_id = []
    artist_id = []
    played_at = []
    album_release_date = []
    album_name = []
    album_id = []
    artist_genre = []

    for song in data["items"]:
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        song_name.append(song["track"]["name"])
        album_name.append(song["track"]["album"]["name"])
        album_release_date.append(song["track"]["album"]["release_date"])
        artist_id.append(song["track"]["album"]["artists"][0]["id"])
        song_id.append(song["track"]["id"])
        album_id.append(song["track"]["album"]["id"])
        song_popularity.append(song["track"]["popularity"])
        played_at.append(song["played_at"])
        

    tracks_dict = {
        "artist_name": artist_name,
        "song_name": song_name,
        "album_name": album_name,
        "album_release_date": album_release_date,
        "artist_id": artist_id,
        "song_id": song_id,
        "album_id": album_id,
        "song_popularity": song_popularity,
        "played_at": played_at
    }

      
    tracks_df = pd.DataFrame(tracks_dict, columns=list(tracks_dict.keys()))

    album_release_dates = tracks_df["album_release_date"].tolist()
    tracks_df["album_release_date"] = [f"{x}-01-01" if len(x) == 4 else x for x in album_release_dates]

    tracks_np = tracks_df.to_numpy()
    track_tuples = [tuple(x) for x in tracks_np]
    cols = ','.join(list(tracks_df.columns))

    db = database.Database()
    # db.create_table()
    db.insert_into_table(tracks_df)
