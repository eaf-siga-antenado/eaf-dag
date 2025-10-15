import csv
import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {}
dag = DAG(
    dag_id="monitor_manutencao_problema_tecnico",
    default_args=default_args,
    schedule_interval="0 7-22/3 * * 1-6",  # A cada 3h das 7h às 22h, segunda a sábado
    catchup=False,
    start_date=datetime(2025, 10, 9),
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

    def only_digits(string):
        return "".join(filter(str.isdigit, string or ""))

    # Configurações MongoDB
    MONGO_CONNECTION_STR = Variable.get("MONGO_CONNECTION_STR_EAF_PRD")
    
    # Configurações Email
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    data_execucao = datetime.now()
    
    # Logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("monitor_manutencao")

    logger.info("🔧 Iniciando Monitor de Manutenção - Problema Técnico...")
    logger.info(f"⏰ Hora da execução: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Conecta ao MongoDB
    logger.info("Conectando ao MongoDB...")
    mongo_client = MongoClient(MONGO_CONNECTION_STR)
    db = mongo_client["eaf"]
    collection = db["tickets"]

    # Busca OS de Manutenção criadas nas últimas 3 horas
    agora = datetime.now(timezone.utc)
    tres_horas_atras = agora - timedelta(hours=3)
    noventa_dias_atras = agora - timedelta(days=90)
    
    logger.info(f"Buscando OSs de Manutenção criadas entre {tres_horas_atras} e {agora}...")
    logger.info(f"🔍 Verificando appointmentEndTime anterior a: {noventa_dias_atras}")

    # Busca OSs de Manutenção - Problema técnico
    cursor_manutencao = collection.find(
        {
            "createdAt": {
                "$gte": tres_horas_atras,
            },
            "services.serviceOrder.type": "Manutenção - Problema técnico"
        },
        {
            "_id": 0,
            "services.serviceOrder.number": 1,
            "services.serviceOrder.type": 1,
            "services.serviceOrder.registeredAt": 1,
            "customer.cpf": 1,
            "createdAt": 1,
        }
    )

    resultados = []
    os_manutencao_processadas = 0
    lista_os_analisadas = []  # Para debug
    
    for doc in cursor_manutencao:
        customer_cpf = doc.get("customer", {}).get("cpf", "NULL")
        
        # Percorre todos os serviços do mesmo documento (OSs vizinhas)
        services = doc.get("services", [])
        
        # Encontra OSs de Manutenção - Problema técnico
        for service in services:
            service_order = service.get("serviceOrder", {})
            os_type = service_order.get("type")
            
            # Verifica se é uma OS de Manutenção - Problema técnico
            if os_type == "Manutenção - Problema técnico":
                os_number = service_order.get("number")
                registered_at = service_order.get("registeredAt")
                
                if not os_number:
                    continue
                    
                os_manutencao_processadas += 1
                clean_os_number = only_digits(str(os_number))
                lista_os_analisadas.append(clean_os_number)  # Para debug
                
                logger.info(f"🔍 Processando OS Manutenção: {clean_os_number}")
                
                # Agora percorre as OSs vizinhas (mesmo documento) em busca de qualquer service order
                logger.info(f"🔍 Analisando {len(services)} service orders vizinhas para OS {clean_os_number}")
                
                for i, service_vizinho in enumerate(services):
                    service_order_vizinho = service_vizinho.get("serviceOrder", {})
                    os_vizinha_number = service_order_vizinho.get("number", "N/A")
                    os_vizinha_type = service_order_vizinho.get("type", "N/A")
                    
                    logger.info(f"📋 Service Order {i+1}: OS {os_vizinha_number}, Tipo: {os_vizinha_type}")
                    
                    installer_response = service_order_vizinho.get("installerResponse", {})
                    appointment_end_time = installer_response.get("appointmentEndTime")
                    
                    logger.info(f"🕐 appointmentEndTime: {appointment_end_time}")
                    
                    if appointment_end_time:
                        # Verifica se appointmentEndTime é anterior a 90 dias atrás (ou seja, > 90 dias atrás)
                        if isinstance(appointment_end_time, datetime):
                            # Ajusta timezone se necessário
                            if appointment_end_time.tzinfo is None:
                                appointment_end_time = appointment_end_time.replace(tzinfo=timezone.utc)
                            
                            # Se appointmentEndTime é anterior a 90 dias atrás
                            if appointment_end_time < noventa_dias_atras:
                                dias_desde_appointment = (agora - appointment_end_time).days
                                logger.info(f"✅ ENCONTRADO: appointmentEndTime de {appointment_end_time} é anterior a 90 dias ({dias_desde_appointment} dias atrás)")
                                
                                resultados.append({
                                    "nro_os_manutencao": clean_os_number,
                                    "registered_at_manutencao": registered_at.strftime("%Y-%m-%d %H:%M:%S") if registered_at else "NULL",
                                    "customer_cpf": customer_cpf,
                                    "os_vizinha_number": os_vizinha_number,
                                    "os_vizinha_type": os_vizinha_type,
                                    "appointment_end_time": appointment_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                    "dias_desde_appointment": dias_desde_appointment
                                })
                                break  # Para evitar duplicatas para a mesma OS de manutenção
                            else:
                                dias_desde_appointment = (agora - appointment_end_time).days
                                logger.info(f"❌ appointmentEndTime de {appointment_end_time} não atende critério ({dias_desde_appointment} dias atrás, precisa ser > 90)")
                    else:
                        logger.info(f"❌ Sem appointmentEndTime para OS {os_vizinha_number}")

    logger.info(f"✅ Processamento concluído:")
    logger.info(f"📊 OSs de Manutenção analisadas: {os_manutencao_processadas}")
    logger.info(f"🔧 OSs com appointmentEndTime > 90 dias: {len(resultados)}")

    # Fecha conexão
    mongo_client.close()

    # Cria diretório de relatórios
    os.makedirs("reports", exist_ok=True)
    
    # Nome do arquivo
    timestamp = data_execucao.strftime('%Y-%m-%d_%H-%M')
    relatorio_file = os.path.join("reports", f"monitor_manutencao_{timestamp}.csv")

    # Gera CSV apenas se há resultados
    if resultados:
        with open(relatorio_file, mode="w", newline='', encoding="utf-8-sig") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=[
                "nro_os_manutencao", "registered_at_manutencao", "customer_cpf", 
                "os_vizinha_number", "os_vizinha_type", "appointment_end_time", "dias_desde_appointment"
            ], delimiter=';')
            writer.writeheader()
            writer.writerows(resultados)
        logger.info(f"📂 Arquivo gerado: {relatorio_file}")
    else:
        logger.info("✅ Nenhuma OS encontrada com os critérios especificados.")
        relatorio_file = None

    # Função para enviar e-mail
    def enviar_email_relatorio():
        try:
            msg = EmailMessage()
            msg["Subject"] = f"🔧 Monitor Manutenção - OSs com appointmentEndTime > 90 dias {data_execucao.strftime('%d/%m/%Y %H:%M')}"
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = "marcelo.ferreira.terceirizado@eaf.org.br"
            
            corpo_email = f"""
Prezado Marcelo,

Segue o relatório do Monitor de Manutenção que identifica OSs de "Manutenção - Problema técnico" 
que possuem service orders vizinhas com appointmentEndTime anterior a 90 dias atrás.

📊 RESUMO DA EXECUÇÃO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}
- OSs de Manutenção analisadas: {os_manutencao_processadas}
- OSs encontradas com critério: {len(resultados)}

🔍 CRITÉRIOS APLICADOS:
- OSs de "Manutenção - Problema técnico" criadas nas últimas 3 horas
- Possui service order vizinha (mesmo ticket) com appointmentEndTime > 90 dias atrás
- Data limite: {noventa_dias_atras.strftime('%d/%m/%Y %H:%M:%S')}

🐛 DEBUG - OSs ANALISADAS:
{', '.join(lista_os_analisadas) if lista_os_analisadas else 'Nenhuma OS encontrada'}

🔍 QUERY MONGODB PARA DEBUG (Compass):
{{
    "createdAt": {{ "$gte": ISODate("{tres_horas_atras.strftime('%Y-%m-%dT%H:%M:%S.000Z')}") }},
    "services.serviceOrder.type": "Manutenção - Problema técnico"
}}

{"📎 Arquivo CSV em anexo com os detalhes." if resultados else "📎 Nenhum arquivo gerado (não há dados para o período)."}
"""

            if resultados:
                corpo_email += f"\n\n🔧 OSs ENCONTRADAS:\n"
                for resultado in resultados[:10]:  # Mostra apenas as primeiras 10 no corpo do email
                    corpo_email += f"- OS Manutenção {resultado['nro_os_manutencao']} (CPF: {resultado['customer_cpf']}) - OS Vizinha: {resultado['os_vizinha_number']} ({resultado['os_vizinha_type']}) - appointmentEndTime há {resultado['dias_desde_appointment']} dias\n"
                if len(resultados) > 10:
                    corpo_email += f"... e mais {len(resultados) - 10} OSs no arquivo anexo.\n"

            msg.set_content(corpo_email)

            # Anexa o arquivo CSV apenas se existir
            if relatorio_file and resultados:
                with open(relatorio_file, "rb") as f:
                    conteudo = f.read()
                    nome_arquivo = os.path.basename(relatorio_file)
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

    # Envia o e-mail
    enviar_email_relatorio()

    logger.info("🔧 Monitor de Manutenção finalizado!")

# Task do Airflow
monitor_manutencao_task = PythonVirtualenvOperator(
    task_id="monitor_manutencao_problema_tecnico",
    python_callable=main,
    requirements=["pymongo==4.10.1"],
    system_site_packages=True,
    dag=dag,
)

monitor_manutencao_task
