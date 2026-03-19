import io
import logging
import paramiko
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

def listar_arquivos_sftp(**context):
    # ──────────────────────────────────────────
    # 🔐 Credenciais via Airflow Variables
    # ──────────────────────────────────────────
    host            = Variable.get("SFTP_HOST")
    port            = int(Variable.get("SFTP_PORT", default_var=22))
    username        = Variable.get("SFTP_USER")
    private_key_str = Variable.get("SFTP_PRIVATE_KEY")
    passphrase      = Variable.get("SFTP_KEY_PASSPHRASE", default_var=None) or None
    remote_dir      = Variable.get("SFTP_UBER_REMOTE_DIR", default_var="from_uber/trips")

    # ──────────────────────────────────────────
    # 📅 Data lógica do DAG — garante idempotência
    # ──────────────────────────────────────────
    execution_date = context["logical_date"]
    yesterday      = execution_date - timedelta(days=1)
    expected_file  = yesterday.strftime("daily_trips-%Y_%m_%d.csv")

    # ──────────────────────────────────────────
    # 🔑 Carrega chave privada a partir da string
    # ──────────────────────────────────────────
    private_key = paramiko.RSAKey.from_private_key(
        io.StringIO(private_key_str),
        password=passphrase,
    )

    # ──────────────────────────────────────────
    # 🔌 Conexão SSH → SFTP
    # ──────────────────────────────────────────
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.RejectPolicy())

    try:
        ssh.connect(
            hostname=host,
            port=port,
            username=username,
            pkey=private_key,
            look_for_keys=False,
            allow_agent=False,
        )

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
    finally:
        ssh.close()


default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": None,
    "retry_delay": None
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
