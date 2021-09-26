# Spotify Recently Played Tracks History
**Spotify Recently Played Tracks History** is an implementation of the data pipeline which extracts the recently played tracks from Spotify API and loads them into PostgreSQL database.

## Requirements
1. [PostgreSQL](https://www.postgresql.org/)
2. [Python 3.8+](https://www.python.org/) 
3. [Spotify for Developers App](https://developer.spotify.com/dashboard/)

## Running project
1. Set Redirect URI in your Spotify App to `http://127.0.0.1:8080/callback/q`.
2. Put your `CLIENT_ID` and `CLIENT_SECRET` in `secrets.py` file or store them directly in `app.py`.
3. Install the dependencies by running `python -m pip install -r requirements.txt`.
4. To setup the PostgreSQL database run the following command: `python database.py setup`.
5. To load the data into the database, run `python app.py`.

## References
This project was inspired by the following videos, webpages and repositories:
- https://www.youtube.com/watch?v=dvviIUKwH7o
- https://www.youtube.com/watch?v=xdq6Gz33khQ
- http://prokulski.science:8501/
- https://github.com/drshrey/spotify-flask-auth-example/blob/master/main.py
