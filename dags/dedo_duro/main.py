import csv
import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {}
dag = DAG(
    dag_id="dedo_duro_comparacao_status",
    default_args=default_args,
    schedule_interval="0 * * * *",  # Executa de hora em hora
    catchup=False,
    start_date=datetime(2025, 10, 8),
)

def main():
    import csv
    import os
    import logging
    from datetime import datetime, timedelta, timezone
    from pymongo import MongoClient
    from sqlalchemy import create_engine
    import pandas as pd
    from airflow.models import Variable
    import smtplib
    from email.message import EmailMessage

    def only_digits(string):
        return "".join(filter(str.isdigit, string or ""))

    # Configurações MongoDB
    MONGO_CONNECTION_STR = Variable.get("MONGO_CONNECTION_STR_EAF_PRD")
    
    # Configurações SQL Server
    username = Variable.get("DBUSER")
    password = Variable.get("DBPASSWORD")
    server = Variable.get("DBSERVER")
    database = Variable.get("DATABASE")
    
    # Configurações Email
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    data_execucao = datetime.now()
    
    # Logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("dedo_duro")

    logger.info("🕵️ Iniciando serviço Dedo Duro - Comparação de Status...")
    
    # Conecta ao MongoDB
    logger.info("Conectando ao MongoDB...")
    mongo_client = MongoClient(MONGO_CONNECTION_STR)
    db = mongo_client["eaf"]
    collection = db["tickets"]

    # Conecta ao SQL Server
    logger.info("Conectando ao SQL Server...")
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+18+for+SQL+Server')

    # Busca OS criadas na última hora no MongoDB
    agora = datetime.now(timezone.utc)
    uma_hora_atras = agora - timedelta(hours=1)
    
    logger.info(f"Buscando OS criadas entre {uma_hora_atras} e {agora}...")

    cursor = collection.find(
        {
            "createdAt": {
                "$gte": uma_hora_atras,
            }
        },
        {
            "_id": 0,
            "services.serviceOrder.number": 1,
            "services.serviceOrder.status": 1,
            "createdAt": 1,
        }
    )

    # Listas para armazenar os resultados
    divergencias_status = []
    os_nao_encontradas = []

    resultados = []
    queries_update = []
    nao_encontradas = []
    lista_os_analisadas = []  # Para debug

    os_processadas = 0
    
    for doc in cursor:
        created_at = doc.get("createdAt")  # Pega o createdAt da raiz do documento
        
        for service in doc.get("services", []):
            service_order = service.get("serviceOrder", {})
            os_number = service_order.get("number")
            mongo_status = service_order.get("status")
            
            if not os_number or not mongo_status:
                continue
                
        os_processadas += 1
        clean_os_number = only_digits(str(os_number))
        lista_os_analisadas.append(clean_os_number)  # Para debug            # Busca a OS no SQL Server
            try:
                query = f"""
                SELECT service_order_status
                FROM eaf_tvro.crm_ticket_data
                WHERE service_order_number = '{clean_os_number}'
                """
                
                sql_result = pd.read_sql(query, engine)
                
                if not sql_result.empty:
                    # OS encontrada no SQL
                    sql_status = sql_result.iloc[0]['service_order_status']
                    logger.info(f"Marcelo debug - OS: {clean_os_number}, Mongo: {mongo_status}, SQL: {sql_status}")
                    
                    # Verifica se os status são diferentes
                    if mongo_status != sql_status:
                        divergencias_status.append({
                            "nro_os": clean_os_number,
                            "status_mongo": mongo_status,
                            "status_sql": sql_status,
                            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else "NULL"
                        })
                        logger.info(f"📊 Divergência encontrada - OS: {clean_os_number}, Mongo: {mongo_status}, SQL: {sql_status}")
                else:
                    # OS não encontrada no SQL
                    os_nao_encontradas.append({
                        "nro_os": clean_os_number,
                        "status_mongo": mongo_status,
                        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else "NULL"
                    })
                    logger.info(f"❌ OS não encontrada no SQL: {clean_os_number}")
                    
            except Exception as e:
                logger.error(f"Erro ao consultar OS {clean_os_number} no SQL: {e}")

    logger.info(f"✅ Processamento concluído: {os_processadas} OSs analisadas")
    logger.info(f"📊 Divergências de status: {len(divergencias_status)}")
    logger.info(f"❌ OSs não encontradas no SQL: {len(os_nao_encontradas)}")

    # Fecha conexões
    mongo_client.close()
    engine.dispose()

    # Cria diretório de relatórios
    os.makedirs("reports", exist_ok=True)
    
    # Nomes dos arquivos
    timestamp = data_execucao.strftime('%Y-%m-%d_%H-%M')
    divergencias_file = os.path.join("reports", f"dedo_duro_divergencias_{timestamp}.csv")
    nao_encontradas_file = os.path.join("reports", f"dedo_duro_nao_encontradas_{timestamp}.csv")

    arquivos_criados = []

    # Gera CSV de divergências de status
    if divergencias_status:
        with open(divergencias_file, mode="w", newline='', encoding="utf-8-sig") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=[
                "nro_os", "status_mongo", "status_sql", "created_at"
            ], delimiter=';')
            writer.writeheader()
            writer.writerows(divergencias_status)
        logger.info(f"📂 Arquivo de divergências gerado: {divergencias_file}")
        arquivos_criados.append(divergencias_file)

    # Gera CSV de OS não encontradas
    if os_nao_encontradas:
        with open(nao_encontradas_file, mode="w", newline='', encoding="utf-8-sig") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=[
                "nro_os", "status_mongo", "created_at"
            ], delimiter=';')
            writer.writeheader()
            writer.writerows(os_nao_encontradas)
        logger.info(f"📂 Arquivo de OS não encontradas gerado: {nao_encontradas_file}")
        arquivos_criados.append(nao_encontradas_file)

    # Função para enviar e-mail
    def enviar_email_relatorio():
        try:
            msg = EmailMessage()
            msg["Subject"] = f"🕵️ Dedo Duro - Relatório de Divergências {data_execucao.strftime('%d/%m/%Y %H:%M')}"
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = "felipe.silva.terceirizado@eaf.org.br, marcelo.ferreira.terceirizado@eaf.org.br"
            
            corpo_email = f"""
Prezado,

Segue o relatório do serviço "Dedo Duro" que monitora divergências entre MongoDB e SQL Server.

📊 RESUMO DA EXECUÇÃO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M')}
- OSs analisadas: {os_processadas}
- Divergências de status: {len(divergencias_status)}
- OSs não encontradas no SQL: {len(os_nao_encontradas)}

🐛 DEBUG - OSs ANALISADAS:
{', '.join(lista_os_analisadas) if lista_os_analisadas else 'Nenhuma OS encontrada'}

{"📎 Arquivos em anexo:" if arquivos_criados else "✅ Nenhuma divergência encontrada neste período."}
"""

            if divergencias_status:
                corpo_email += f"\n\n🔍 DIVERGÊNCIAS ENCONTRADAS:\n"
                for div in divergencias_status[:10]:  # Mostra apenas as primeiras 10 no corpo do email
                    corpo_email += f"- OS {div['nro_os']}: Mongo={div['status_mongo']} vs SQL={div['status_sql']}\n"
                if len(divergencias_status) > 10:
                    corpo_email += f"... e mais {len(divergencias_status) - 10} divergências no arquivo anexo.\n"

            msg.set_content(corpo_email)

            # Anexa arquivos se existirem
            for arquivo in arquivos_criados:
                with open(arquivo, "rb") as f:
                    conteudo = f.read()
                    nome_arquivo = os.path.basename(arquivo)
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

    #if len(arquivos_criados) > 0:
    enviar_email_relatorio()

    logger.info("🕵️ Serviço Dedo Duro finalizado!")

# Task do Airflow
dedo_duro_task = PythonVirtualenvOperator(
    task_id="comparacao_status_mongo_sql",
    python_callable=main,
    requirements=["pymongo==4.10.1", "sqlalchemy", "pyodbc", "pandas"],
    system_site_packages=True,
    dag=dag,
)

dedo_duro_task
