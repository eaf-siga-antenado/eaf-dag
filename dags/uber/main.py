import logging
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime

log = logging.getLogger(__name__)

def mostrar_conexao():
    conn_id = "uber_conexao"
    
    # 🔌 Recupera a conexão do Airflow
    conn = BaseHook.get_connection(conn_id)
    
    log.info("🔎 Connection ID: %s", conn.conn_id)
    log.info("🌐 Host: %s", conn.host)
    log.info("👤 Login: %s", conn.login)
    log.info("🚪 Port: %s", conn.port)
    log.info("🗂️ Schema: %s", conn.schema)
    
    # ⚠️ Cuidado com senha em logs
    log.info("🔑 Password: %s", conn.password)

    # 📦 Extras (JSON)
    extras = conn.extra_dejson
    log.info("📦 Extras:")
    for key, value in extras.items():
        log.info("  %s: %s", key, value)


default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="debug_conexao_uber",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["debug"],
) as dag:

    task_mostrar_conexao = PythonOperator(
        task_id="mostrar_conexao",
        python_callable=mostrar_conexao,
    )

    task_mostrar_conexao
    