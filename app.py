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
        track_popularity = []
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
                track_popularity.append(song["track"]["popularity"])
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
            "track_popularity": track_popularity,
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
        df = df.drop_duplicates(subset=["artist_id"])
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
        artist_genres_df = pd.DataFrame(
            {"artist_id": artist_id_list,
            "artist_popularity": artist_popularity,
            "followers": artist_followers,
            "artist_genres": artist_genres
            })
        
        return artist_genres_df


    def get_track_features(self, df: DataFrame) -> DataFrame:
        track_features = []
        df = df.drop_duplicates(subset=["track_id"])
        track_list = df["track_id"].tolist()
        audio_features_base_url = f"{self.BASE_URL}/audio-features/"

        for id in track_list:
            audio_features_url = f"{audio_features_base_url}{id}"
            r = requests.get(audio_features_url, headers=self.headers)
            if r.status_code not in range(200, 299):
                self.get_track_features(df=df)
            features = r.json()
            track_features.append(features)
        
        track_features_df = pd.DataFrame(track_features)
        track_features_df = track_features_df.rename(columns={"id": "track_id"})
        track_features_df = track_features_df.drop(columns=["uri", 
                                                            "track_href", 
                                                            "analysis_url", 
                                                            "duration_ms", 
                                                            "type"])
        
        return track_features_df


    def join_all_tracks_data(self) -> DataFrame:
        recently_played = self.get_recently_played()
        artist_data = self.get_artist_data(recently_played)
        track_features = self.get_track_features(recently_played)

        all_data = pd.merge(recently_played, artist_data, on="artist_id", how="left")
        print(f"all_data type: {type(all_data)}")
        print(f"track_features type: {type(track_features)}")
        all_data = pd.merge(all_data, track_features, on="track_id", how="inner")

        return all_data
        

    def clean_df(self, df: DataFrame) -> DataFrame:
    # correct date if release date precision is year
        df["album_release_date"] = [f"{x}-01-01" if len(x) == 4 else x for x in df["album_release_date"].tolist()]
    # correct date if release date precision is month
        df["album_release_date"] = [f"{x}-01" if len(x) == 7 else x for x in df["album_release_date"].tolist()]
    # fill the genre with default value if empty
        df["artist_genres"] = ["<unknown>" if len(x) == 0 else x for x in df["artist_genres"].tolist()]

        return df


    def check_if_data_valid(self, df: DataFrame) -> bool:
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
        
        return True

    def get_tracks_data(self) -> DataFrame:
        tracks_dataset = self.join_all_tracks_data()
        clean_tracks_dataset = self.clean_df(tracks_dataset)

        if not self.check_if_data_valid(clean_tracks_dataset):
            return pd.DataFrame()
        
        return clean_tracks_dataset


  
if __name__ == "__main__":
    auth_code = get_auth_code.obtain_auth_code()
    print(f"The authorization code is: {auth_code}")
    token = get_auth_code.get_token()

    client = SpotifyAPI(token)
    df = client.get_tracks_data()
    db = database.Database()

    
    # albums
    db.insert_into_table(df[["album_id", "album_name", "album_release_date"]], "albums")
    
    # artists
    db.insert_into_table(df[["artist_id", "artist_name", "artist_popularity", "followers"]], "artists")

    # track_info
    db.insert_into_table(df[["track_id",
						"track_name",
						"track_popularity",
						"danceability",
						"energy",
						"key",
						"loudness",
						"mode",
						"speechiness",
						"acousticness",
						"instrumentalness",
						"liveness",
						"valence",
						"tempo",
						"duration_ms",
						"time_signature",
                        "is_explicit"]], "track_info")
    
    # artists_genres
    artist_genres_wide = df["artist_genres"].str.split("," , expand=True)
    artist_genres = pd.concat([df[["artist_id"]], artist_genres_wide], axis=1)
    artist_genres_long = artist_genres.melt(id_vars="artist_id")
    artist_genres_long = artist_genres_long.drop(columns=["variable"])
    artist_genres_long = artist_genres_long.dropna()
    artist_genres_long = artist_genres_long.rename(columns={"value": "genre_name"})
    db.insert_into_table(artist_genres_long[["artist_id"]], "artists")
    db.insert_into_table(artist_genres_long[["genre_name"]], "genres")
    db.insert_into_table(artist_genres_long, "artists_genres")

    # recently_played
    db.insert_into_table(df[["played_at", "track_id", "album_id", "artist_id"]], "recent_tracks")
    