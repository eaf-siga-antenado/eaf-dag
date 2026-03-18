import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def mostrar_diretorio():
    import os
    
    print("Diretório atual (cwd):", os.getcwd())
    print("Arquivos no diretório:")
    print(os.listdir(os.getcwd()))

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': 0
}

dag = DAG(
    'uber',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

task_mostrar_diretorio = PythonOperator(
    task_id='mostrar_diretorio',
    python_callable=mostrar_diretorio,
    dag=dag
)

task_mostrar_diretorio
