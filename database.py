import datetime
import sys

from datetime import date
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pandas.core.frame import DataFrame
import pandas as pd


class Database:
    DB_NAME = "spotify_tracks_history"
    USER = "postgres"
    PASSWORD = "postgres"
    HOST = "127.0.0.1"
    PORT = "5432"
    connection = None
    
    DB_PARAMS = {
                "database": f"{DB_NAME}", 
                "user": "postgres", 
                "password": "postgres", 
                "host": "127.0.0.1", 
                "port": "5432"
                }

    def __init__(self, params=DB_PARAMS):
        try:
            self.connection = connect(**params)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.DatabaseError) as connection_error:
            print(connection_error)
            sys.exit(1)

        
    def create_database(self):
        sql_db_exists = f"SELECT 1 FROM pg_catalog.pg_database pd WHERE datname = '{self.DB_NAME}';"
        self.cursor.execute(sql_db_exists)
        exists = self.cursor.fetchone()
        if not exists:
            try:
                sql = f"""CREATE DATABASE {self.DB_NAME}"""
                self.cursor.execute(sql)
                print("Database created succesfully.")

            except (Exception, psycopg2.DatabaseError) as error:
                print(error)


    def create_table(self, 
                    cols_dict: dict, 
                    table_name: str):

        cols_str = ", ".join([f"{key} {value}" for key, value in cols_dict.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"

        try:
            self.cursor.execute(sql)
            print(f"Table {table_name} created succesfully.")
        except (Exception, psycopg2.OperationalError) as error:
            print(f"Table not created. Error code: {error.pgcode}")


    def alter_table(self, 
                    table_name: str, 
                    constraint_name: str, 
                    constraint_type: str,  
                    column: str):
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_type} ({column});
            """
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")


    def add_pk(self, 
                table_name: str, 
                constraint_name: str,  
                column: str):
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE;
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({column});
            """
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")


    def add_fk(self, 
                table_name: str, 
                constraint_name: str, 
                column: str, 
                table_name_fk: str,
                column_fk: str):
                
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE;
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column}) REFERENCES {table_name_fk} ({column_fk});
            """
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")


    def insert_into_table(self, data: DataFrame, table_name: str):
        df_numpy = data.to_numpy()
        df_tuples = [tuple(row) for row in list(df_numpy)]
        cols = ','.join(list(data.columns))
        query = "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING".format(table_name, cols)
        try:
            execute_values(self.cursor, query, df_tuples)
            self.connection.commit()
        except(psycopg2.IntegrityError) as error:
            print(f"Error ocured during insert into table {table_name}. Error code: {error.pgcode}")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            self.connection.rollback()

    def count_records(self):
        query = "SELECT COUNT(*) FROM recent_tracks"
        try:
            self.cursor.execute(query)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Query could not be executed. Error code: {error}")

        records = self.cursor.fetchall()

        today_records_query = f"""SELECT COUNT(*) 
                                   FROM recent_tracks 
                                   WHERE EXTRACT(YEAR FROM played_at) || '-' || EXTRACT(MONTH FROM played_at) || '-' || EXTRACT(DAY FROM played_at) = '{date.today()}'"""
        
        try:
            self.cursor.execute(today_records_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Query could not be executed. Error code: {error}")
        
        today_records = self.cursor.fetchall()
        print(f"{today_records[0][0]} new records have been added today.")
        print(f"Your database contains {records[0][0]} records in total.")


    def __del__(self):
        self.cursor.close()
        self.connection.close()


if __name__ == "__main__":

    # If you run "python database.py setup", the program will connect do the default postgres database
    # and create new database with parameters given in the class Database.
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        db_create_params = {
                "database": "postgres", 
                "user": "postgres", 
                "password": "postgres", 
                "host": "127.0.0.1", 
                "port": "5432"
                }

        db_setup = Database(db_create_params)
        db_setup.create_database()
        db_spotify = Database()
 
        # Define columns in the database tables.
        recent_track_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "played_at": "timestamp",
            "track_id" :"TEXT", 
            "album_id" : "TEXT",
            "artist_id": "TEXT"
            }

        albums_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "album_id": "TEXT",
            "album_name": "TEXT",
            "album_release_date": "timestamp"
            }

        artists_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "artist_id": "TEXT",
            "artist_name": "TEXT",
            "artist_popularity": "integer",
            "followers": "integer"
        }

        genres_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "genre_name": "TEXT"
        }

        artists_genres_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "artist_id": "TEXT",
            "genre_name": "TEXT"
        }

        track_info_cols = {
            "id": "integer GENERATED ALWAYS AS IDENTITY",
            "track_id": "TEXT",
            "track_name": "TEXT",
            "track_popularity": "integer",
            "danceability": "decimal",
            "energy": "decimal",
            "key": "decimal", 
            "loudness": "decimal",
            "mode": "decimal",
            "speechiness": "decimal",
            "acousticness": "decimal",
            "instrumentalness": "decimal",
            "liveness": "decimal",
            "valence": "decimal",
            "tempo": "decimal",
            "duration_ms": "integer",
            "time_signature": "integer",
            "is_explicit": "bool"
        }

        # Create tables in the database.
        db_spotify.create_table(recent_track_cols, "recent_tracks")
        db_spotify.create_table(albums_cols, "albums")
        db_spotify.create_table(artists_cols, "artists") 
        db_spotify.create_table(genres_cols, "genres")
        db_spotify.create_table(artists_cols, "artists")
        db_spotify.create_table(artists_genres_cols, "artists_genres")
        db_spotify.create_table(track_info_cols, "track_info")

        # Add Primary Keys.
        db_spotify.add_pk("albums", "album_id_pk", "album_id")
        db_spotify.add_pk("artists", "artist_id_pk", "artist_id")
        db_spotify.add_pk("genres", "genre_name_pk", "genre_name")
        db_spotify.add_pk("track_info", "track_id_pk", "track_id")
        db_spotify.add_pk("recent_tracks", "played_at_pk", "played_at")

        # Add Foreign Keys.
        db_spotify.add_fk("recent_tracks", "track_id_fk", "track_id", "track_info", "track_id")
        db_spotify.add_fk("recent_tracks", "album_id_fk", "album_id", "albums", "album_id")
        db_spotify.add_fk("recent_tracks", "artist_id_fk", "artist_id", "artists", "artist_id")
        db_spotify.add_fk("artists_genres", "artist_id_fk", "artist_id", "artists", "artist_id")
        db_spotify.add_fk("artists_genres", "genre_name_fk", "genre_name", "genres", "genre_name")