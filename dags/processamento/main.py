import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

def informacoes():
    print(f'local que estou: {os.getcwd()}')
    print(f'quantidade de núcleos que a maquina possui: {os.cpu_count()}')

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
}

dag = DAG(
    'processamento',
    default_args=default_args,
    schedule_interval='20 11 * * *',
    catchup=False
)

informacoes = PythonOperator(
    task_id='informacoes',
    python_callable=informacoes,
    dag=dag
)

informacoes
