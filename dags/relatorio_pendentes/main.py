import csv
import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {}
dag = DAG(
    dag_id="os_pendentes_5h",
    default_args=default_args,
    schedule_interval="0 */2 * * *",  # Executa a cada 2h
    catchup=False,
    start_date=datetime(2025, 8, 12),
)

def main():
    import csv
    import os
    import logging
    from datetime import datetime, timedelta, timezone
    from pymongo import MongoClient
    from airflow.models import Variable
    import smtplib
    from email.message import EmailMessage

    # Verificar se está no horário e dia permitido
    agora = datetime.now()
    dia_semana = agora.weekday()  # 0=segunda, 1=terça, ..., 6=domingo
    hora = agora.hour
    
    if dia_semana > 4 or hora < 7 or hora > 22:  # Segunda a sexta (0-4), 7h às 22h
        logging.info(f"Execução fora do horário permitido. Dia: {dia_semana}, Hora: {hora}")
        return

    # Configurações
    MONGO_CONNECTION_STR = Variable.get("MONGO_CONNECTION_STR_EAF_PRD")
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    data_execucao = datetime.now()
    OUTPUT_CSV_PATH = f"os_pendentes_5h_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.csv"

    # Logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("listar_os_pendentes")

    logger.info("Conectando ao MongoDB...")
    client = MongoClient(MONGO_CONNECTION_STR)
    db = client["eaf"]
    collection = db["tickets"]

    logger.info("Buscando OS pendentes há mais de 5 horas...")

    agora = datetime.now(timezone.utc)
    cinco_horas_atras = agora - timedelta(hours=5)

    status_alvo = ["PENDING", "OPEN", "IN_VALIDATION", "VALIDATION", "INSTALLER_INTEGRATION_ERROR"]

    cursor = collection.find(
        {
            "services.serviceOrder.status": {"$in": status_alvo}
        },
        {
            "_id": 0,
            "services.serviceOrder.number": 1,
            "services.serviceOrder.type": 1,
            "services.serviceOrder.status": 1,
            "services.serviceOrder.registeredAt": 1,
            "services.serviceOrder.installer": 1,
            "services.serviceOrder.failureReason": 1,
            "services.serviceOrder.createdBy": 1,
            "services.serviceOrder.schedules": 1,
            "createdBy": 1
        }
    )

    resultados = []
    for doc in cursor:
        for so in doc.get("services", []):
            service_order = so.get("serviceOrder", {})
            schedule = service_order.get("schedules", [{}])[0]
            status = service_order.get("status")
            registeredAt = service_order.get("registeredAt")

            # Corrige timezone
            if isinstance(registeredAt, datetime) and registeredAt.tzinfo is None:
                registeredAt = registeredAt.replace(tzinfo=timezone.utc)

            # Aplica filtro
            if status in status_alvo and registeredAt and registeredAt <= cinco_horas_atras:
                resultados.append({
                    "nro OS": service_order.get("number", "NULL"),
                    "type": service_order.get("type", "NULL"),
                    "status": status,
                    "registeredAt": registeredAt.strftime("%Y-%m-%d %H:%M:%S") if registeredAt else "NULL",
                    "installer": schedule.get("installer"),
                    "failureReason": service_order.get("failureReason", "NULL"),
                    "createdBy": doc.get("createdBy", "NULL")
                })

    logger.info(f"Encontradas {len(resultados)} OS para exportar...")

    if len(resultados) == 0:
        return

    os.makedirs("reports", exist_ok=True)
    output_path = os.path.join("reports", OUTPUT_CSV_PATH)
    with open(output_path, mode="w", newline='', encoding="utf-8-sig") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=[
            "nro OS", "type", "status", "registeredAt", "installer", "failureReason", "createdBy"
        ])
        writer.writeheader()
        writer.writerows(resultados)

    logger.info(f"Arquivo salvo: {output_path}")
    client.close()

    # ===== Função para enviar e-mail =====
    def enviar_email_com_csv(caminho_csv, destinatarios, assunto, corpo):
        try:
            msg = EmailMessage()
            msg["Subject"] = assunto
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = ", ".join(destinatarios)
            msg.set_content(corpo)

            with open(caminho_csv, "rb") as f:
                conteudo = f.read()
                nome_arquivo = os.path.basename(caminho_csv)
                msg.add_attachment(
                    conteudo,
                    maintype="application",
                    subtype="octet-stream",
                    filename=nome_arquivo,
                )

            with smtplib.SMTP("smtp.office365.com", 587) as smtp:
                smtp.starttls()
                smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
                smtp.send_message(msg)

            logger.info("✅ E-mail enviado com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao enviar e-mail: {e}")

    # Envia o e-mail com o relatório
    enviar_email_com_csv(
        output_path,
        ["felipe.silva.terceirizado@eaf.org.br"],
        assunto="CRM > Alerta OS para análise",
        corpo=f"""
Prezados,
Anexo o registro automático das Ordens de Serviço que estão pendentes há mais de 5 horas no CRM EAF.
Data de execução: {data_execucao.strftime('%d/%m/%Y %H:%M')}
"""
    )

os_pendentes_5h_task = PythonVirtualenvOperator(
    task_id="listar_os_pendentes_5h",
    python_callable=main,
    requirements=["pymongo==4.10.1"],
    system_site_packages=True,
    dag=dag,
)

os_pendentes_5h_task

