import os
import csv
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable
from airflow.models.param import Param
# ✅ Importa JSON para lidar com os parâmetros string
import json 

default_args = {}

dag = DAG(
    dag_id="os_check_manual_detalhado",
    default_args=default_args,
    schedule_interval=None,  # Executa apenas manualmente
    catchup=False,
    start_date=datetime(2025, 9, 30),
    # Define os parâmetros para a UI de Trigger
    params={
        "os_list": Param(
            type="array",
            title="Lista de OSs",
            description="Lista de números de Ordem de Serviço (OS) a serem verificados.",
            default=["1234567"],
            minItems=1,
            uniqueItems=True,
            items={"type": "string"}
        ),
        "destinatarios": Param(
            type="array",
            title="E-mails Destinatários",
            description="Lista de e-mails para onde o relatório será enviado.",
            default=["usuario@exemplo.com"],
            minItems=1,
            uniqueItems=True,
            items={"type": "string", "format": "email"}
        ),
        "tipo_export": Param(
            type="string",
            title="Tipo de Export",
            description="Escolha o tipo de arquivo a ser gerado: CSV ou UPDATE SQL.",
            default="csv",
            enum=["csv", "update_sql"]
        ),
    }
)

# A função agora espera os_list, destinatarios e tipo_export como strings JSON (que serão convertidas)
def main(os_list=None, destinatarios=None, tipo_export=None):
    # Imports internos (necessários para o PythonVirtualenvOperator)
    import os
    import csv
    import logging
    import datetime
    from pymongo import MongoClient
    from airflow.models import Variable
    import smtplib
    from email.message import EmailMessage
    # ✅ Importa JSON aqui também, dentro do escopo da função
    import json 

    def only_digits(string):
        return "".join(filter(str.isdigit, string or ""))

    def format_value(v):
        if v is None or v == "":
            return "NULL"
        if isinstance(v, datetime.datetime):
            return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
        if isinstance(v, str):
            # Substitui aspas simples por duas aspas simples para evitar quebra de string SQL
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        return f"'{str(v)}'"

    def converte_ticket_para_update(ticket, service_order_number):
        service = next(
            (s for s in ticket.get("services", [])
             if s.get("serviceOrder", {}).get("number") == service_order_number),
            {}
        )
        protocol = service.get("protocol", {})
        service_order = service.get("serviceOrder", {})
        installer_response = service_order.get("installerResponse", {}) or {}

        cad_unico = ticket.get("customer", {}).get("cadUnico", {})
        address = ticket.get("customer", {}).get("address", {})
        contacts = ticket.get("customer", {}).get("contacts", [])

        row = {
            "protocol_number": only_digits(protocol.get("number")),
            "protocol_created_at": protocol.get("createdAt"),
            "protocol_channel": protocol.get("channel"),
            "service_order_status": service_order.get("status"),
            "service_order_type": service_order.get("type"),
            "service_order_created_at": service_order.get("registeredAt"),
            "schedule_date_time": service_order.get("schedules", [{}])[0].get("date"),
            "installer": service_order.get("schedules", [{}])[0].get("installer"),
            "schedule_installer": service_order.get("schedules", [{}])[0].get("installer"),
            "installer_response_appointment_end_time": installer_response.get("appointmentEndTime"),
            "installer_response_equipment_serial_number": installer_response.get("equipmentSerialNumber"),
            "installer_response_classification": installer_response.get("classification"),
            "last_modified_date": ticket.get("lastModifiedDate"),
            "customer_name": ticket.get("customer", {}).get("name"),
            "customer_cpf": ticket.get("customer", {}).get("cpf"),
            "customer_phone": contacts[0].get("value") if contacts else None,
            "customer_family_code": cad_unico.get("familyCode"),
            "customer_nis": cad_unico.get("nis"),
            "customer_email": ticket.get("customer", {}).get("email"),
            "customer_ibge": cad_unico.get("ibgeCode"),
            "customer_postal_code": address.get("postalCode"),
            "customer_full_address": f"{address.get('streetName')}, {address.get('number')} - {address.get('complement')} - {address.get('neighborhood')} - {address.get('postalCode')} , {address.get('city')}-{address.get('state')}",
            "customer_address_reference": address.get("reference"),
            "tabulation": service.get("tabulation"),
            "failure_reason": service.get("failureReason", "N/A"),
            "synched_at": datetime.datetime.now(),
            "installer_response_appointment_start_time": installer_response.get("appointmentStartTime"),
            "installer_response_representant_cpf": installer_response.get("representantCpf"),
            "installer_response_representant_name": installer_response.get("representantName"),
            "installer_response_evidence": installer_response.get("evidence", {}).get("fileUrl"),
            "installer_response_technician_id": installer_response.get("technicianId"),
            "installer_response_caid_equipment": installer_response.get("caidEquipamento"),
            "installer_response_scid_equipment": installer_response.get("scidEquipamento"),
            "installer_response_lat_long_installation": installer_response.get("latlongInstalacao"),
            "installer_response_pta": installer_response.get("pta")
        }

        set_clause = ", ".join(f"[{col}] = {format_value(val)}" for col, val in row.items())
        where_clause = f"[service_order_number] = '{only_digits(service_order_number)}'"
        return f"UPDATE eaf_tvro.crm_ticket_data SET {set_clause} WHERE {where_clause};"

    # ✅ TRATAMENTO DE PARÂMETROS STRING (JSON) PARA LISTA
    # O PythonVirtualenvOperator passa os op_kwargs como strings.
    # Convertemos de volta para list/array Python.
    try:
        if isinstance(os_list, str):
            # O Jinja pode envolver os elementos em aspas simples; substituímos por aspas duplas para JSON.
            os_list = json.loads(os_list.replace("'", '"')) 
            
        if isinstance(destinatarios, str):
            destinatarios = json.loads(destinatarios.replace("'", '"'))
            
        if isinstance(tipo_export, str):
            tipo_export = tipo_export.strip('"\'')  # Remove aspas extras se houver
    except json.JSONDecodeError as e:
        # Se a conversão falhar, levanta um erro claro
        raise ValueError(f"Erro ao decodificar parâmetro JSON. Verifique a sintaxe. Erro: {e}")

    # Define o tipo de export padrão se não fornecido
    if not tipo_export:
        tipo_export = "csv"

    # Configurações
    MONGO_CONNECTION_STR = Variable.get("MONGO_CONNECTION_STR_EAF_PRD")
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    if not os_list or not destinatarios:
        # Este check agora é muito importante, caso a conversão resulte em None ou lista vazia.
        raise ValueError("É necessário fornecer parâmetros: os_list e destinatarios.")

    # A linha abaixo foi removida pois o json.loads já garante que 'destinatarios' é uma lista
    # if isinstance(destinatarios, str):
    #     destinatarios = [destinatarios] 

    data_execucao = datetime.datetime.now()
    os.makedirs("reports", exist_ok=True)

    # Define nomes de arquivos baseados no tipo de export
    if tipo_export == "update_sql":
        output_file = os.path.join("reports", f"update_installer_fields_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.txt")
    else:
        output_file = os.path.join("reports", f"os_check_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.csv")
    
    not_found_file = os.path.join("reports", f"os_nao_encontradas_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.txt")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("os_check_manual_detalhado")

    logger.info(f"🔎 Verificando {len(os_list)} OSs para export do tipo: {tipo_export}...")

    client = MongoClient(MONGO_CONNECTION_STR)
    db = client["eaf"]
    tickets = db["tickets"]

    resultados = []
    queries_update = []
    nao_encontradas = []

    for os_number in os_list:
        clean_number = only_digits(str(os_number))
        ticket = tickets.find_one({"services.serviceOrder.number": clean_number})

        if ticket:
            # Extrai service e serviceOrder corretos
            service = next((s for s in ticket.get("services", [])
                             if s.get("serviceOrder", {}).get("number") == clean_number), {})
            
            if tipo_export == "update_sql":
                # Gera comando UPDATE SQL
                try:
                    update_sql = converte_ticket_para_update(ticket, clean_number)
                    queries_update.append(update_sql)
                except Exception as e:
                    logger.error(f"Erro ao gerar UPDATE para OS {clean_number}: {e}")
            else:
                # Gera dados para CSV (lógica original)
                protocol = service.get("protocol", {})
                service_order = service.get("serviceOrder", {})
                installer_response = service_order.get("installerResponse", {}) or {}

                cad_unico = ticket.get("customer", {}).get("cadUnico", {})
                address = ticket.get("customer", {}).get("address", {})
                contacts = ticket.get("customer", {}).get("contacts", [])

                row = {
                    "protocol_number": only_digits(protocol.get("number")),
                    "protocol_created_at": protocol.get("createdAt"),
                    "protocol_channel": protocol.get("channel"),
                    "service_order_status": service_order.get("status"),
                    "service_order_type": service_order.get("type"),
                    "service_order_created_at": service_order.get("registeredAt"),
                    "schedule_date_time": service_order.get("schedules", [{}])[0].get("date"),
                    "installer": service_order.get("schedules", [{}])[0].get("installer"),
                    "schedule_installer": service_order.get("schedules", [{}])[0].get("installer"),
                    "installer_response_appointment_end_time": installer_response.get("appointmentEndTime"),
                    "installer_response_equipment_serial_number": installer_response.get("equipmentSerialNumber"),
                    "installer_response_classification": installer_response.get("classification"),
                    "last_modified_date": ticket.get("lastModifiedDate"),
                    "customer_name": ticket.get("customer", {}).get("name"),
                    "customer_cpf": ticket.get("customer", {}).get("cpf"),
                    "customer_phone": contacts[0].get("value") if contacts else None,
                    "customer_family_code": cad_unico.get("familyCode"),
                    "customer_nis": cad_unico.get("nis"),
                    "customer_email": ticket.get("customer", {}).get("email"),
                    "customer_ibge": cad_unico.get("ibgeCode"),
                    "customer_postal_code": address.get("postalCode"),
                    "customer_full_address": f"{address.get('streetName')}, {address.get('number')} - {address.get('complement')} - {address.get('neighborhood')} - {address.get('postalCode')} , {address.get('city')}-{address.get('state')}",
                    "customer_address_reference": address.get("reference"),
                    "tabulation": service.get("tabulation"),
                    "failure_reason": service.get("failureReason", "N/A"),
                    "synched_at": datetime.datetime.now(),
                    "installer_response_appointment_start_time": installer_response.get("appointmentStartTime"),
                    "installer_response_representant_cpf": installer_response.get("representantCpf"),
                    "installer_response_representant_name": installer_response.get("representantName"),
                    "installer_response_evidence": installer_response.get("evidence", {}).get("fileUrl"),
                    "installer_response_technician_id": installer_response.get("technicianId"),
                    "installer_response_caid_equipment": installer_response.get("caidEquipamento"),
                    "installer_response_scid_equipment": installer_response.get("scidEquipamento"),
                    "installer_response_lat_long_installation": installer_response.get("latlongInstalacao"),
                    "installer_response_pta": installer_response.get("pta")
                }

                resultados.append(row)
        else:
            nao_encontradas.append(clean_number)

    # Grava arquivo principal baseado no tipo de export
    if tipo_export == "update_sql":
        if queries_update:
            with open(output_file, "w", encoding="utf-8") as f:
                for query in queries_update:
                    f.write(query + "\n")
            logger.info(f"📂 Arquivo UPDATE SQL gerado: {output_file}")
    else:
        # Grava CSV (lógica original)
        if resultados:
            with open(output_file, mode="w", newline='', encoding="utf-8-sig") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=list(resultados[0].keys()), delimiter=';') 
                writer.writeheader()
                writer.writerows(resultados)
            logger.info(f"📂 CSV gerado: {output_file}")

    # Grava TXT para não encontradas
    if nao_encontradas:
        with open(not_found_file, "w", encoding="utf-8") as f:
            f.write("OSs não encontradas no MongoDB:\n")
            for osn in nao_encontradas:
                f.write(f"{osn}\n")
        logger.info(f"📂 TXT gerado: {not_found_file}")

    client.close()

    # ===== Enviar e-mail =====
    try:
        msg = EmailMessage()
        msg["Subject"] = f"CRM > Consulta OS Manual {'- UPDATE SQL' if tipo_export == 'update_sql' else '- Detalhada'}"
        msg["From"] = EMAIL_REMETENTE
        msg["To"] = ", ".join(destinatarios) # Destinatarios é garantido ser uma lista aqui
        msg.set_content(f"""
Prezados,
Segue em anexo o resultado da consulta manual de OS no CRM EAF {'(comandos UPDATE SQL)' if tipo_export == 'update_sql' else '(dados detalhados)'}.
Data de execução: {data_execucao.strftime('%d/%m/%Y %H:%M')}
""")

        # Anexa arquivo principal
        if (tipo_export == "update_sql" and queries_update) or (tipo_export == "csv" and resultados):
            with open(output_file, "rb") as f:
                msg.add_attachment(f.read(), maintype="application", subtype="octet-stream", filename=os.path.basename(output_file))

        # Anexa TXT se existir
        if nao_encontradas:
            with open(not_found_file, "rb") as f:
                msg.add_attachment(f.read(), maintype="text", subtype="plain", filename=os.path.basename(not_found_file))

        with smtplib.SMTP("smtp.office365.com", 587) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
            smtp.send_message(msg)

        logger.info("✅ E-mail enviado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar e-mail: {e}")


# Operator
os_check_detalhado_task = PythonVirtualenvOperator(
    task_id="os_check_detalhado",
    python_callable=main,
    requirements=["pymongo==4.10.1"],
    system_site_packages=True,
    # ✅ Passa os parâmetros como strings JSON (o padrão do Airflow para o Virtualenv)
    op_kwargs={
        "os_list": "{{ dag_run.conf['os_list'] }}",
        "destinatarios": "{{ dag_run.conf['destinatarios'] }}",
        "tipo_export": "{{ dag_run.conf['tipo_export'] }}",
    },
    dag=dag,
)

os_check_detalhado_task
