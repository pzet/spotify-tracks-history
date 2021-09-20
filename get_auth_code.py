from flask import Flask, redirect, request
import webbrowser
from urllib.parse import urlencode
import requests
from secrets import CLIENT_ID, CLIENT_SECRET
import json
import os


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



if __name__ == "__main__":
    auth_code = obtain_auth_code()
    print(auth_code)
