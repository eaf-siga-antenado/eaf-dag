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
    
    # Conecta ao MongoDB
    logger.info("Conectando ao MongoDB...")
    mongo_client = MongoClient(MONGO_CONNECTION_STR)
    db = mongo_client["eaf"]
    collection = db["tickets"]

    # Busca OS de Manutenção criadas nas últimas 3 horas
    agora = datetime.now(timezone.utc)
    tres_horas_atras = agora - timedelta(hours=3)
    
    logger.info(f"Buscando OSs de Manutenção criadas entre {tres_horas_atras} e {agora}...")

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
                
                # Agora percorre as OSs vizinhas (mesmo documento) em busca de Agendamento de instalação
                for service_vizinho in services:
                    service_order_vizinho = service_vizinho.get("serviceOrder", {})
                    
                    if (service_order_vizinho.get("type") == "Agendamento de instalação" and 
                        service_order_vizinho.get("status") == "INSTALLED"):
                        
                        installer_response = service_order_vizinho.get("installerResponse", {})
                        appointment_end_time = installer_response.get("appointmentEndTime")
                        
                        if appointment_end_time and registered_at:
                            # Verifica se a OS de manutenção foi aberta com MENOS de 90 dias da instalação
                            if isinstance(appointment_end_time, datetime) and isinstance(registered_at, datetime):
                                # Ajusta timezone se necessário
                                if appointment_end_time.tzinfo is None:
                                    appointment_end_time = appointment_end_time.replace(tzinfo=timezone.utc)
                                if registered_at.tzinfo is None:
                                    registered_at = registered_at.replace(tzinfo=timezone.utc)
                                
                                # Calcula a diferença entre a data de instalação e a abertura da manutenção
                                diferenca_dias = (registered_at - appointment_end_time).days
                                
                                # Se a manutenção foi aberta em MENOS de 90 dias após a instalação
                                if 0 <= diferenca_dias < 90:
                                    logger.info(f"📊 OS encontrada: Manutenção {clean_os_number} aberta {diferenca_dias} dias após instalação")
                                    
                                    resultados.append({
                                        "nro_os_manutencao": clean_os_number,
                                        "registered_at_manutencao": registered_at.strftime("%Y-%m-%d %H:%M:%S"),
                                        "customer_cpf": customer_cpf,
                                        "os_instalacao_relacionada": service_order_vizinho.get("number"),
                                        "appointment_end_time": appointment_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                        "dias_entre_instalacao_manutencao": diferenca_dias
                                    })
                                    break  # Para evitar duplicatas para a mesma OS de manutenção

    logger.info(f"✅ Processamento concluído:")
    logger.info(f"📊 OSs de Manutenção analisadas: {os_manutencao_processadas}")
    logger.info(f"🔧 OSs com instalações antigas (+90 dias): {len(resultados)}")

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
                "os_instalacao_relacionada", "appointment_end_time", "dias_entre_instalacao_manutencao"
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
            msg["Subject"] = f"🔧 Monitor Manutenção - OSs abertas < 90 dias da instalação {data_execucao.strftime('%d/%m/%Y %H:%M')}"
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = "felipe.silva.terceirizado@eaf.org.br, marcelo.ferreira.terceirizado@eaf.org.br"
            
            corpo_email = f"""
Prezado,

Segue o relatório do Monitor de Manutenção que identifica OSs de "Manutenção - Problema técnico" 
abertas em menos de 90 dias após uma instalação.

📊 RESUMO DA EXECUÇÃO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M')}
- OSs de Manutenção analisadas: {os_manutencao_processadas}
- OSs encontradas com critério: {len(resultados)}

🔍 CRITÉRIOS APLICADOS:
- OSs de "Manutenção - Problema técnico" criadas nas últimas 3 horas
- No mesmo ticket, existe OS de "Agendamento de instalação" com status INSTALLED
- Manutenção foi aberta em MENOS de 90 dias após o appointmentEndTime da instalação

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
                    corpo_email += f"- OS Manutenção {resultado['nro_os_manutencao']} (CPF: {resultado['customer_cpf']}) - Aberta {resultado['dias_entre_instalacao_manutencao']} dias após instalação\n"
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
