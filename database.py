import sys

from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pandas.core.frame import DataFrame
import pandas as pd


class Database:
    DB_NAME = "spotify_tracks_history"
    TABLE_NAME = "recently_played_tracks"
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


    def create_table(self, cols_dict: dict, table_name: str):
        cols_str = ", ".join([f"{key} {value}" for key, value in cols_dict.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_str})"
        try:
            self.cursor.execute(sql)
            print(f"Table {table_name} created succesfully.")
        except (Exception, psycopg2.OperationalError) as error:
            print(f"Table not created. Error code: {error.pgcode}")


    def alter_table(self, table_name: str, constraint_name: str, constraint_type: str,  column: str):
        try:
            sql = f"""
            ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};
            ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_type} ({column});
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
            print(f"Duplicated records omitted. Error code: {error.pgcode}")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            self.connection.rollback()

  
    def __del__(self):
        self.cursor.close()
        self.connection.close()


if __name__ == "__main__":
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
        
        tracks_history_cols = {
            "artist_name": "TEXT", 
            "song_name": "TEXT", 
            "album_name": "TEXT",
            "album_release_date": "DATE",
            "artist_id": "TEXT",
            "song_id": "TEXT",
            "album_id": "TEXT",
            "song_popularity": "INTEGER",
            "played_at": "TIMESTAMP"
            }

        artist_data_cols = {
            "artist_id": "TEXT",
            "artist_popularity": "INTEGER",
            "artist_genres": "TEXT"
            }
        
        db_spotify.create_table(tracks_history_cols, "track_history")
        db_spotify.create_table(artist_data_cols, "artist_data")
        db_spotify.alter_table("track_history", "played_at_pk", "PRIMARY KEY", "played_at")
        db_spotify.alter_table("artist_data", "artist_id_pk", "PRIMARY KEY", "artist_id")


    
