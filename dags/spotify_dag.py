from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

file_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(file_dir)
sys.path.append(parent_dir)
from spotify_etl import run_spotify_etl

default_args = {
    'owner'             :   'airflow',
    'depends_on_past'   :   False,
    'start_date'        :   datetime(2022, 2, 4),
    'email'             :   ['ppzet9@gmail.com'],
    'email_on_failure'  :   True,
    'email_on_retries'  :   True,
    'retries'           :   1,
    'retry_delay'       :   timedelta(minutes=1)
}

def simple_printer():
    print('Test output to see if DAG works correctly.')

dag = DAG(
    'get_spotify_tracks_history_dag',
    default_args=default_args,
    description='Fetches recently played spotify tracks, collects the artist, album and track data, transform it and loads into the OTLP database.',
    schedule_interval=timedelta(days=1) 
)

run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable=run_spotify_etl,
    dag=dag
)

run_etl
