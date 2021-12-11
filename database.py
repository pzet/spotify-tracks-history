import sys
from datetime import date
import os

from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pandas.core.frame import DataFrame
import pandas as pd
import dotenv   


# Load database parameters as environmental variables.
dotenv.load_dotenv()

class Database:
    connection = None
    
    DB_PARAMS = {
                "database": os.environ["database"], 
                "user": os.environ["user"], 
                "password": os.environ["password"], 
                "host": os.environ["host"], 
                "port": os.environ["port"]
                }

    def __init__(self, params=DB_PARAMS):
        try:
            self.connection = self.connect_to_db(params)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1)

        
    def connect_to_db(self, params: dict):
        """Connect to the PostgreSQL database server."""
        conn = None
        try:
            print("Connecting to the PostgreSQL database...")
            conn = connect(**params)
            print("Connection succesful.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1)

        return conn


    def create_database(self):
        """
        Execute a CREATE DATABASE request 
        if the dabase doesn't already exists.
        """
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


    def create_table(self, cols_dict: dict, table_name: str):
        """Execute a single CREATE TABLE request."""
        cols_str = ", ".join([f"{key} {value}" for key, value in cols_dict.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"

        try:
            self.cursor.execute(sql)
            print(f"Table {table_name} created succesfully.")
        except (Exception, psycopg2.OperationalError) as error:
            print(f"Table not created. Error code: {error.pgcode}")


    def alter_table(
        self, 
        table_name: str, 
        constraint_name: str, 
        constraint_type: str,  
        column: str
        ):
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_type} ({column});
            """
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")


    def add_pk(self, table_name: str, constraint_name: str, column: str):
        """Sets PRIMARY KEY on the existing table."""
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE;
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({column});
            """
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")


    def add_fk(
        self, 
        table_name: str, 
        constraint_name: str, 
        column: str, 
        table_name_fk: str,
        column_fk: str
    ):
        """Sets FOREIGN KEY on existing tables."""
        sql = f"""
                ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE;
                ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column}) REFERENCES {table_name_fk} ({column_fk});
                """
        try:
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")

    
    def add_constraint_unique(self, table_name: str, constraint_name: str, *columns):
        columns_str = ", ".join(columns[0])
        """Add UNIQUE constraint to assure unique values in column(s)."""
        sql = f"""ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({columns_str})"""
        try:
            self.cursor.execute(sql)
        except(psycopg2.ProgrammingError) as error:
            print(f"Error ocured: {error}\nError code: {error.pgcode}")

    def insert_into_table(self, data: DataFrame, table_name: str):
        """Inserts data into table."""
        df_numpy = data.to_numpy()
        df_tuples = [tuple(row) for row in list(df_numpy)]
        cols = ','.join(list(data.columns))
        query = "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING".format(table_name, cols)
        try:
            execute_values(self.cursor, query, df_tuples)
            self.connection.commit()
        except(psycopg2.IntegrityError) as error:
            print(f"""Error occurred during insert into table {table_name}. 
                      Error code: {error.pgcode}""")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            self.connection.rollback()


    def count_records(self):
        """
        Counts records added to the database in the current date 
        and the overall number of records in the database.
        """
        query = "SELECT COUNT(*) FROM recent_tracks"
        try:
            self.cursor.execute(query)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Query could not be executed. Error code: {error}")

        records = self.cursor.fetchall()

        today_records_query = f"""
                                SELECT COUNT(*)
                                FROM recent_tracks 
                                WHERE EXTRACT(YEAR FROM played_at) 
                                   || '-' 
                                   || EXTRACT(MONTH FROM played_at) 
                                   || '-' 
                                   || EXTRACT(DAY FROM played_at) = '{str(date.today()).replace('-0', '-')}'
                                """
        
        try:
            self.cursor.execute(today_records_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Query could not be executed. Error code: {error}")
        
        today_records = self.cursor.fetchall()
        print(f"You have listened to {today_records[0][0]} records today.")
        print(f"Your listening history contains {records[0][0]} records in total.")


    def __del__(self):
        try:
            self.cursor.close()
            self.connection.close()
        except AttributeError as error:
            print(f"""{error}.\nIt seems that connection to the database 
                      could not be established.""")

if __name__ == "__main__":

    # If you run "python database.py setup", the program will connect 
    # to the database and create all tables and constraints.

    if len(sys.argv) > 1 and sys.argv[1] == "setup":

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

        # Add UNIQUE constraint.
        db_spotify.add_constraint_unique("artists_genres", "unique_artist_genre", ["artist_id", "genre_name"])