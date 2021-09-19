import datetime
import base64
import json

from flask import Flask, redirect, request
import webbrowser
from urllib.parse import urlencode
import requests
import pandas as pd

# Comment this line if you input CLEINT_ID and CLIENT_SECRET below
from secrets import CLIENT_ID, CLIENT_SECRET

app = Flask(__name__)

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


def extract_auth_code():
    auth_token = request.args["code"]
    global AUTH_TOKEN
    AUTH_TOKEN = auth_token
    r = requests.get(f"{CLIENT_SIDE_URL}:{PORT}/shutdown")
    token_msg = f"Your authorization code is: <br \>{auth_token}"
    return token_msg


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def get_client_creds_b64():
    """Returns credentials as b64 encoded string."""
    client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    client_creds_b64 = base64.b64encode(client_creds.encode())
    return client_creds_b64.decode()


def get_token_data():
    """Returns access tokes as JSON format."""
    client_creds_b64 = get_client_creds_b64()
    token_url = SPOTIFY_TOKEN_URL
    token_query_params = {
        "grant_type": "authorization_code",
        "code": f"{AUTH_TOKEN}",
        "redirect_uri": REDIRECT_URI
    }
    token_headers = {"Authorization": f"Basic {client_creds_b64}"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=token_query_params, headers=token_headers)
    if r.status_code not in range(200, 299):
        print(r.json())
        raise Exception(f"Authentication failed. Requests status code: {r.status_code}")
    token_data = r.json()
    token_data_to_json_file(token_data)
    return token_data["access_token"]

def token_data_to_json_file(token_data):
    with open("aaa.txt", mode="w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=0)

def get_token_data_from_json_file():
    with open("secrets.txt", mode='r') as f:
        json.load(f)
    
def get_recently_played(token):
    endpoint = "https://api.spotify.com/v1/me/player/recently-played"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {"limit": 50}
    r = requests.get(endpoint, headers=headers, params=payload)
    if r.status_code not in range(200, 299):
        print(r.json())
        raise Exception("Could not get requested user data.")
    return r.json()

def get_artist_genres(artist_id):
    artist_genres = []
    artist_base_url = "https://api.spotify.com/v1/artists/"
    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
    }

    for id in artist_id:
        artist_url = f"{artist_base_url}{id}"
        r = requests.get(artist_url, headers=headers)
        if r.status_code not in range(200, 299):
            return []
        genres = r.json()["genres"]
        artist_genres.append(genres)
        
    return artist_genres

# def get_track_features(track_id):
#     track_features = []
#     audio_features_base_url = "https://api.spotify.com/v1/audio-features/"
#     headers = {
#     "Accept": "application/json",
#     "Content-Type": "application/json",
#     "Authorization": f"Bearer {token}"
#     }

#     for id in artist_id:
#         audio_features_url = f"{audio_features_base_url}{id}"
#         r = requests.get(audio_features_url, headers=headers)
#         if r.status_code not in range(200, 299):
#             return []
#         genres = r.json()["genres"]
#         track_features.append(genres)

#     return track_features


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
        

@app.route("/")
def index():
    auth_query_parameters = {
    "response_type": "code",
    "redirect_uri": REDIRECT_URI,
    "scope": SCOPE,
    # "state": STATE,
    "show_dialog": SHOW_DIALOG,
    "client_id": CLIENT_ID
}
    auth_url_params = f"{SPOTIFY_AUTH_URL}?{urlencode(auth_query_parameters)}"
    return redirect(auth_url_params)


@app.route("/callback/q")
def callback():
    return extract_auth_code()
    
 
@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'
    
if __name__ == "__main__":
    webbrowser.open_new(f"{CLIENT_SIDE_URL}:{PORT}")
    app.run(debug=False, port=PORT)
    # print(AUTH_TOKEN)
    token = get_token_data()
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
        
    artist_genre = get_artist_genres(artist_id)

    tracks_dict = {
        "artist_name": artist_name,
        "song_name": song_name,
        "album_name": album_name,
        "album_release_date": album_release_date,
        "artist_id": artist_id,
        "song_id": song_id,
        "album_id": album_id,
        "song_popularity": song_popularity,
        "played_at": played_at,
        "artist_genre": artist_genre
    }

      
    tracks_df = pd.DataFrame(tracks_dict, columns=list(tracks_dict.keys()))

    

    print(filter_by_played_at(tracks_df))

