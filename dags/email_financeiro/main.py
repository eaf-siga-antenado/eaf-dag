from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator

def extrair_dados_api():
    import requests
    import pandas as pd
    from io import StringIO
    from airflow.models import Variable
    from sqlalchemy import create_engine

    df = pd.read_csv(StringIO(requests.get('https://api-financial-system-stg.sigaantenado.com.br/financial-system/api/daily-sheet').text))
    
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    df.to_sql("email_financeiro", engine, if_exists='append', schema='eaf_tvro', index=False)

default_args = {
    'start_date': datetime(2023, 8, 18, 5, 0, 0)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'email_financeiro',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=False
)

extrair_dados_api = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_api,
    dag=dag
)

extrair_dados_api 
