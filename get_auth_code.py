import base64
import datetime
import webbrowser
import json
import requests
import os 
from flask import Flask, redirect, request
from urllib.parse import urlencode
import dotenv


# Spotify URLs
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"

# Server-side parameters
CLIENT_SIDE_URL = "http://127.0.0.1"
PORT = "8080"
REDIRECT_URI = f"{CLIENT_SIDE_URL}:{PORT}/callback/q"

# Authentication query parameters
SCOPE = "user-read-recently-played"
STATE = ""
SHOW_DIALOG = "false"

# Load Client ID and Client Secret as environmental variables.
dotenv.load_dotenv()

app = Flask(__name__)

def json_not_contains(token_type: str) -> bool:
    # Check if secrets.json contains authorization code or token
    try:
        secrets = read_json()
    except ValueError: 
        with open("secrets.json", "w", encoding="utf-8") as f:
            secrets = {}
            json.dump(secrets, f)

    if token_type not in secrets.keys():
        return True
    elif token_type in secrets.keys() and len(secrets[token_type]) == 0:
        return True

    return False

def read_json(secrets_filename="secrets.json", encoding="utf-8") -> json:
    """Reads the content of JSON file."""
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    file_dir = os.path.join(cur_dir, secrets_filename)
    with open(file_dir) as f:
        content = json.load(f)
    
    return content

def is_token_expired() -> bool:
    """Check if the token is already expired."""
    token_data = read_json()
    expiration_time = token_data["expires_at"]
    expiration_time_datetime = datetime.datetime.strptime(expiration_time, ("%m/%d/%Y, %H:%M:%S"))
    now = datetime.datetime.now()
    
    return now > expiration_time_datetime


def auth_code_to_json(auth_code: str, secrets_filename='secrets.json') -> None:
    """Write authorization code to the secrets.json file"""
    auth_code_dict = {
            "authorization_code": auth_code
        }
    
    secrets = read_json()
    secrets.update(auth_code_dict)
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    file_dir = os.path.join(cur_dir, secrets_filename)

    with open(file_dir, "w", encoding="utf-8") as f:
        json.dump(secrets, f, indent=0)
        

def extract_auth_code() -> str:
    """
    Reads authorization code from request arguments, writes the code to JSON file
    and shuts the server down (with GET request).
    """
    auth_code = request.args["code"]
    _ = requests.get(f"{CLIENT_SIDE_URL}:{PORT}/shutdown")
    auth_code_to_json(auth_code)

    return auth_code


def obtain_auth_code() -> str:
    """Gets an authorization code if it's missing from secrets.json file."""
    if json_not_contains("authorization_code"):
        webbrowser.open_new(f"{CLIENT_SIDE_URL}:{PORT}")
        app.run(debug=False, port=PORT)
    
    secrets_dict = read_json()
    
    return secrets_dict["authorization_code"]



def get_client_creds_b64() -> str:
    """Returns client credentials as b64 encoded string."""
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]
    client_creds = f"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(client_creds.encode())

    return client_creds_b64.decode()


def get_token() -> str:
    """"Requests new token if one is missing from secrets.json file 
        or refreshes the token if it's expired."""
    if json_not_contains("access_token"):
        request_token()
        print("New token obtained.")
    if is_token_expired():
        refresh_token()
        print("Token has been refreshed.")
    
    token_data = read_json()
    access_token = token_data["access_token"]
    
    return access_token
    

def request_token() -> str:
    """Performs a request for the authorization token. Returns the access token in JSON format."""
    client_creds_b64 = get_client_creds_b64()
    auth_code = extract_auth_code()
    data = {
        "grant_type": "authorization_code",
        "code": f"{auth_code}",
        "redirect_uri": REDIRECT_URI
    }
    headers = {"Authorization": f"Basic {client_creds_b64}"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers)
    if r.status_code not in range(200, 299):
        print(r.json())
        raise Exception(f"Authentication failed. Request status code: {r.status_code}")
    token_data = r.json()
    token_expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=token_data["expires_in"])
    token_expiration_time = token_expiration_time.strftime("%m/%d/%Y, %H:%M:%S")
    token_data["expires_at"] = token_expiration_time
    token_data_to_json_file(token_data)
    
    return token_data["access_token"]


def refresh_token(secrets_filename='secrets.json'):
    """Refreshes token and updates it in the secrets.json file."""
    token_data = read_json()
    refresh_token = token_data["refresh_token"]
    client_credentials = get_client_creds_b64()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    headers = {
        "Authorization": f"Basic {client_credentials}"
    }

    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers)
    new_token_data = r.json()
    refreshed_token = new_token_data["access_token"]
    token_data["access_token"] = refreshed_token
    token_expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=token_data["expires_in"])
    token_expiration_time = token_expiration_time.strftime("%m/%d/%Y, %H:%M:%S")
    token_data["expires_at"] = token_expiration_time

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    file_dir = os.path.join(cur_dir, secrets_filename)

    with open(file_dir, "w") as f:
        json.dump(token_data, f, indent=0)


def token_data_to_json_file(token_data, secrets_filename='secrets.json'):
    """Writes token data into JSON file."""
    secrets_json = read_json()
    secrets_json.update(token_data)
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    file_dir = os.path.join(cur_dir, secrets_filename)


    with open(file_dir, mode="w", encoding="utf-8") as f:
        json.dump(secrets_json, f, indent=0)


# Flask app
@app.route("/")
def index() -> redirect:
    auth_query_parameters = {
    "response_type": "code",
    "redirect_uri": REDIRECT_URI,
    "scope": SCOPE,
    # "state": STATE,
    "show_dialog": SHOW_DIALOG,
    "client_id": os.environ["CLIENT_ID"]
}
    auth_url_params = f"{SPOTIFY_AUTH_URL}?{urlencode(auth_query_parameters)}"
    return redirect(auth_url_params)


@app.route("/callback/q")
def callback():
    return request_token()
    
 
@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'


def shutdown_server():
    """Shuts the server down when GET method is invoked."""
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

if __name__ == "__main__":
    auth_code = obtain_auth_code()
    # print(get_token())
    # print(auth_code)
    # print(is_token_expired())
