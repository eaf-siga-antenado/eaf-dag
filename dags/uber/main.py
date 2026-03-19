from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def obter_ip_saida():
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=10)
        ip = response.json().get("ip")
        print(f"IP de saída: {ip}")
    except Exception as e:
        print(f"Erro ao obter IP: {e}")


with DAG(
    dag_id="obter_ip_saida",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # executa sob demanda
    catchup=False,
    tags=["util", "network"],
) as dag:

    tarefa_obter_ip = PythonOperator(
        task_id="obter_ip",
        python_callable=obter_ip_saida,
    )

    tarefa_obter_ip
    