import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def local():
    import os
    print(os.getcwd())
   
default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'uber',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

local = PythonOperator(
    task_id='local',
    python_callable=local,
    dag=dag
) 

local 
