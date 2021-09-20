import base64
import datetime
import webbrowser
import json
import os
import requests

from flask import Flask, redirect, request
from urllib.parse import urlencode

from secrets import CLIENT_ID, CLIENT_SECRET


app = Flask(__name__)

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

def is_auth_code_empty():
    # Check if secrets.json contains authorization code
    try:
        with open("secrets.json", encoding="utf-8") as f:
            secrets = json.load(f)
    except ValueError: 
        with open("secrets.json", "w", encoding="utf-8") as f:
            secrets = {}
            json.dump(secrets, f)

    if "authorization_code" not in secrets.keys():
        return True
    elif "authorization_code" in secrets.keys() and len(secrets["authorization_code"]) == 0:
        return True

    return False

def is_token_empty():
    # Check if secrets.json contains authorization code
    try:
        with open("secrets.json", encoding="utf-8") as f:
            secrets = json.load(f)
    except ValueError: 
        with open("secrets.json", "w", encoding="utf-8") as f:
            secrets = {}
            json.dump(secrets, f)

    if "access_token" not in secrets.keys():
        return True
    elif "access_token" in secrets.keys() and len(secrets["access_token"]) == 0:
        return True

    return False


def is_token_expired():
    now = datetime.datetime.now()
    
    with open("secrets.json") as f:
        token_data = json.load(f)
    
    expiration_time = token_data["expires_at"]
    expiration_time_datetime = datetime.datetime.strptime(expiration_time, ("%m/%d/%Y, %H:%M:%S"))

    return now > expiration_time_datetime


def auth_code_to_json(auth_code):
    # Write authorization code to the secrets.json file
    auth_code_dict = {
            "authorization_code": auth_code
        }
    
    with open("secrets.json", encoding="utf-8") as f:
        secrets = json.load(f)
    
    secrets.update(auth_code_dict)

    with open("secrets.json", "w", encoding="utf-8") as f:
        json.dump(secrets, f, indent=0)
        

def extract_auth_code():
    auth_token = request.args["code"]
    global AUTH_TOKEN
    AUTH_TOKEN = auth_token
    r = requests.get(f"{CLIENT_SIDE_URL}:{PORT}/shutdown")
    auth_code_to_json(auth_token)
    token_msg = f"Your authorization code is: <br \>{auth_token}"
    return token_msg


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


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


def obtain_auth_code():
    if is_auth_code_empty():
        webbrowser.open_new(f"{CLIENT_SIDE_URL}:{PORT}")
        app.run(debug=False, port=PORT)
    
    with open("secrets.json") as f:
        secrets_dict = json.load(f)
    
    return secrets_dict["authorization_code"]


def get_client_creds_b64():
    """Returns credentials as b64 encoded string."""
    client_creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    client_creds_b64 = base64.b64encode(client_creds.encode())
    return client_creds_b64.decode()


def get_token():
    if is_token_empty():
        request_token()
    if is_token_expired():
        refresh_token()
    
    with open("secrets.json") as f:
        token_data = json.load(f)

    access_token = token_data["access_token"]
    
    return access_token
    

def request_token():
    """Returns access tokes as JSON format."""
    client_creds_b64 = get_client_creds_b64()
    token_url = SPOTIFY_TOKEN_URL
    data = {
        "grant_type": "authorization_code",
        "code": f"{AUTH_TOKEN}",
        "redirect_uri": REDIRECT_URI
    }
    headers = {"Authorization": f"Basic {client_creds_b64}"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers)
    if r.status_code not in range(200, 299):
        print(r.json())
        raise Exception(f"Authentication failed. Requests status code: {r.status_code}")
    token_data = r.json()
    token_expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=token_data["expires_in"])
    token_expiration_time = token_expiration_time.strftime("%m/%d/%Y, %H:%M:%S")
    token_data["expires_at"] = token_expiration_time
    token_data_to_json_file(token_data)


def refresh_token():
    token_url = "https://accounts.spotify.com/api/token"

    with open("secrets.json") as f:
        token_data = json.load(f)

    refresh_token = token_data["refresh_token"]
    client_credentials = get_client_creds_b64()

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    headers = {
        "Authorization": f"Basic {client_credentials}"
    }

    r = requests.post(token_url, data=data, headers=headers)
    new_token_data = r.json()
    print(new_token_data)
    refreshed_token = new_token_data["access_token"]
    token_data["access_token"] = refreshed_token
    token_expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=token_data["expires_in"])
    token_expiration_time = token_expiration_time.strftime("%m/%d/%Y, %H:%M:%S")
    token_data["expires_at"] = token_expiration_time

    with open("secrets.json", "w") as f:
        json.dump(token_data, f, indent=0)


def token_data_to_json_file(token_data):
    with open("secrets.json", encoding="utf-8") as f:
        secrets_json = json.load(f)

    secrets_json.update(token_data)
    
    with open("secrets.json", mode="w", encoding="utf-8") as f:
        json.dump(secrets_json, f, indent=0)


# def get_token_data_from_json_file():
#     if not is_token_expired():
#         try:
#             with open("secrets.txt", mode='r') as f:
#                 json.load(f)
#         except ValueError:
#             print('Error while reading token data from JSON file.')



    

if __name__ == "__main__":
    auth_code = obtain_auth_code()
    print(get_token())
    print(auth_code)
    print(is_token_expired())
