from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator

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
    'telegram',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)

enviar_mensagem = PythonOperator(
    task_id='extrair_dados',
    python_callable=enviar_mensagem,
    dag=dag
)

enviar_mensagem 
