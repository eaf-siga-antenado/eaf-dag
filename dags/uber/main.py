import logging
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

log = logging.getLogger(__name__)

def listar_arquivos_sftp(**context):
    execution_date = context["logical_date"]
    yesterday = execution_date - timedelta(days=1)
    expected_file = yesterday.strftime("daily_trips-%Y_%m_%d.csv")
    remote_dir = Variable.get("SFTP_UBER_REMOTE_DIR", default_var="from_uber/trips")
    hook = SSHHook(ssh_conn_id="uber_conexao")

    with hook.get_conn() as ssh:
        ssh.set_missing_host_key_policy(__import__("paramiko").RejectPolicy())

        with ssh.open_sftp() as sftp:
            log.info("Conectado ao SFTP. Listando '%s'...", remote_dir)

            try:
                files = sftp.listdir(remote_dir)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Diretório remoto não encontrado: {remote_dir}"
                )

            log.info("Total de arquivos encontrados: %d", len(files))
            for f in files:
                log.info("  • %s", f)

            log.info("Procurando arquivo esperado: %s", expected_file)

            if expected_file in files:
                log.info("✅ Arquivo encontrado: %s", expected_file)
            else:
                raise FileNotFoundError(
                    f"Arquivo esperado não encontrado: {expected_file} "
                    f"(data de referência: {yesterday.date()})"
                )

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sftp_uber_trips",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sftp", "uber"],
) as dag:

    task_listar_sftp = PythonOperator(
        task_id="listar_arquivos_sftp",
        python_callable=listar_arquivos_sftp,
    )
