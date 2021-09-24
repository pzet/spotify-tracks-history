import sys

from psycopg2 import connect, sql
from psycopg2.extras import execute_values

import psycopg2
from psycopg2.extensions import  ISOLATION_LEVEL_AUTOCOMMIT
from pandas.core.frame import DataFrame
import pandas as pd


class Database:

    connection = None

    PARAMS_DICT = {
        "database": "spotify_tracks_history", 
        "user": "postgres", 
        "password": "postgres", 
        "host": "127.0.0.1", 
        "port": "5432"
    }

    def __init__(self):
        try:
            self.connection = connect(**self.PARAMS_DICT)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.connection.autocommit = True
        except (Exception, psycopg2.DatabaseError) as connection_error:
            print(connection_error)
            sys.exit(1)

        self.cursor = self.connection.cursor()


    def create_table(self):
        sql = """
                CREATE TABLE IF NOT EXISTS recently_played_tracks (
                artist_name TEXT, 
                song_name TEXT, 
                album_name TEXT,
                album_release_date DATE,
                artist_id TEXT,
                song_id TEXT,
                album_id TEXT,
                song_popularity INTEGER,
                played_at TIMESTAMP,
                CONSTRAINT played_at_pk PRIMARY KEY(played_at)
                )
            """
        print("Table created succesfully.")
        self.cursor.execute(sql)


    def insert_into_table(self, df: DataFrame, table="recently_played_tracks"):
        df_numpy = df.to_numpy()
        df_tuples = [tuple(row) for row in list(df_numpy)]
        cols = ','.join(list(df.columns))
        query = "INSERT INTO {} ({}) VALUES %s".format(table, cols)
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
        db = Database()
        db.create_table()
