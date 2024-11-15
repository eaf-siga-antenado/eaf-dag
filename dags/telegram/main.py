from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator

def extrair_dados():
    import psycopg2
    import pandas as pd
    from airflow.models import Variable

    conn_params = {
    'dbname': Variable.get('dbname'),
    'user': Variable.get('user'),
    'password': Variable.get('password'),
    'host': Variable.get('host'),
    'port': Variable.get('porte')
}

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    query = """
    SELECT
        installer."name" AS Instaladora,
        CAST(capacity AS VARCHAR) as Instalacoes_dia,
        ibge.city_code ibge
    FROM public.setup setup
    left join public.installer installer
    on setup.id_installer  = installer.id
    left join public.city_ibge ibge on
    ibge.id = setup.id_city
    """
    cursor.execute(query)
    colunas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=colunas)
    print(len(df))
    df.head(10)

def enviar_mensagem():
    import requests
    from airflow.models import Variable

    chat_id = Variable.get('chat_id')
    token = Variable.get('token_telegram')
    message = 'mensagem enviada utilizando o airflow!'
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url)

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
}

dag = DAG(
    'setup_instaladora',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)

enviar_mensagem = PythonOperator(
    task_id='extrairasa_dados',
    python_callable=enviar_mensagem,
    dag=dag
)

extrair_dados = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados,
    dag=dag
)

extrair_dados 
