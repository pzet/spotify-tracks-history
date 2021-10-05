import datetime
import base64
import json
from pandas.core.frame import DataFrame
from psycopg2.extensions import JSON

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
# AUTH_TOKEN = None

class SpotifyAPI():

    API_VERSION = "v1"
    BASE_URL = f"https://api.spotify.com/{API_VERSION}"

    def __init__(self, token):
        self.token = token
        self.headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {self.token}"
    }

    def get_recently_played(self):
        endpoint = f"{self.BASE_URL}/me/player/recently-played"
        params = {"limit": 50} 
        r = requests.get(endpoint, headers=self.headers, params=params)
        if r.status_code not in range(200, 299):
            print(r.json())
            raise Exception("Could not get requested user data.")
        
        recently_played_json = r.json()
        
        artist_name = []
        duration_ms = []
        is_explicit = []
        track_name = []
        song_popularity = []
        track_id = []
        artist_id = []
        played_at = []
        album_release_date = []
        album_name = []
        album_id = []
        artist_genre = []
        
        for song in recently_played_json["items"]:
                artist_name.append(song["track"]["album"]["artists"][0]["name"])
                track_name.append(song["track"]["name"])
                album_name.append(song["track"]["album"]["name"])
                album_release_date.append(song["track"]["album"]["release_date"])
                artist_id.append(song["track"]["album"]["artists"][0]["id"])
                track_id.append(song["track"]["id"])
                album_id.append(song["track"]["album"]["id"])
                song_popularity.append(song["track"]["popularity"])
                played_at.append(song["played_at"])
                duration_ms.append(song["track"]["duration_ms"])
                is_explicit.append(song["track"]["explicit"])
        

        tracks_dict = {
            "artist_name": artist_name,
            "track_name": track_name,
            "album_name": album_name,
            "album_release_date": album_release_date,
            "artist_id": artist_id,
            "track_id": track_id,
            "album_id": album_id,
            "song_popularity": song_popularity,
            "played_at": played_at,
            "duration_ms": duration_ms,
            "is_explicit": is_explicit
        }

      
        tracks_df = pd.DataFrame(tracks_dict, columns=list(tracks_dict.keys()))
    
        return tracks_df

    def get_artist_data(self, df: DataFrame) -> DataFrame:
        artist_genres = []
        artist_popularity = []
        artist_followers = []
        artist_id_list = df["artist_id"].tolist()
        artist_base_url = f"{self.BASE_URL}/artists"

        for id in artist_id_list:
            artist_url = f"{artist_base_url}/{id}"
            r = requests.get(artist_url, headers=self.headers)
            if r.status_code not in range(200, 299):
                return []
            genres = r.json()["genres"]
            popularity = r.json()["popularity"]
            followers = r.json()["followers"]["total"]
            artist_genres.append(genres)
            artist_popularity.append(popularity)
            artist_followers.append(followers)
            
        artist_genres = [",".join(x) for x in artist_genres]
        artist_genres = ["<unknown>" if len(x) == 0 else x for x in artist_genres]
        
        artist_genres_df = pd.DataFrame(
            {"artist_id": artist_id_list,
            "popularity": artist_popularity,
            "followers": artist_followers,
            "artist_genres": artist_genres
            })
        
        return artist_genres_df


    def get_track_features(self, df: DataFrame) -> DataFrame:
        track_features = []
        track_list = df["track_id"].tolist()
        audio_features_base_url = f"{self.BASE_URL}/audio-features/"

        for id in track_list:
            audio_features_url = f"{audio_features_base_url}{id}"
            r = requests.get(audio_features_url, headers=self.headers)
            if r.status_code not in range(200, 299):
                return []
            features = r.json()
            track_features.append(features)
        
        track_features_df = pd.DataFrame(track_features)
        track_features_df = track_features_df.rename(columns={"id": "track_id"})
        track_features_df = track_features_df.drop(columns=["uri", "track_href", "analysis_url", "duration_ms"])
        
        return track_features_df

    def join_all_tracks_data(self) -> DataFrame:
        recently_played = self.get_recently_played()
        artist_data = self.get_artist_data(recently_played)
        track_features = self.get_track_features(recently_played)

        all_data = pd.merge(recently_played, artist_data, on="artist_id", how="left")
        all_data = pd.merge(all_data, track_features, on="track_id", how="inner")

        return all_data.columns
        

    def clean_df(self, df: DataFrame) -> DataFrame:
        pass
        # check the dates
        # sth like:
        # df["artist_genres"] = pd.where(len(df["artist_genres"]) == 0, "<unknown>")    
## end of SpotifyAPI class


def check_if_data_valid(df: DataFrame) -> bool:
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
  
if __name__ == "__main__":
    auth_code = get_auth_code.obtain_auth_code()
    print(f"The authorization code is: {auth_code}")
    token = get_auth_code.get_token()

    # data cleaning
    # tracks_df["album_release_date"] = [f"{x}-01-01" if len(x) == 4 else x for x in album_release_dates]

    # tracks_np = tracks_df.to_numpy()
    # track_tuples = [tuple(x) for x in tracks_np]
    # cols = ','.join(list(tracks_df.columns))


    db = database.Database()
    # db.insert_into_table(tracks_df, "track_history")

    # artist_genres_df = get_artist_data(tracks_df)
    # db.insert_into_table(artist_genres_df, "artist_data")

    client = SpotifyAPI(token)
    recently_played = client.get_recently_played()
    # print(recently_played)
    # print(client.get_track_features(recently_played))
    # print(client.get_artist_data(recently_played))
    print(client.join_all_tracks_data())