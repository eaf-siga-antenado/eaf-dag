import os
import csv
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable
from airflow.models.param import Param # 💡 Importante: Importa o objeto Param

default_args = {}

# 1. ✅ Adicione o argumento 'params' ao DAG
dag = DAG(
    dag_id="os_check_manual_detalhado",
    default_args=default_args,
    schedule_interval=None,  # Executa apenas manualmente
    catchup=False,
    start_date=datetime(2025, 9, 30),
    # Define os parâmetros de entrada. Eles aparecerão na UI de trigger.
    params={
        "os_list": Param(
            type="array",
            title="Lista de OSs",
            description="Lista de números de Ordem de Serviço (OS) a serem verificados.",
            default=["1234567", "7654321"], # Valor de exemplo
            minItems=1,
            uniqueItems=True,
            items={"type": "string"}
        ),
        "destinatarios": Param(
            type="array",
            title="E-mails Destinatários",
            description="Lista de e-mails para onde o relatório será enviado.",
            default=["usuario@exemplo.com"], # Valor de exemplo
            minItems=1,
            uniqueItems=True,
            items={"type": "string", "format": "email"}
        ),
    }
)

# A função main permanece inalterada, pois já aceita os_list e destinatarios como argumentos.
def main(os_list=None, destinatarios=None):
    # ... [O corpo da sua função main() permanece o mesmo aqui] ...
    import os
    import csv
    import logging
    import datetime
    from pymongo import MongoClient
    from airflow.models import Variable
    import smtplib
    from email.message import EmailMessage

    def only_digits(string):
        return "".join(filter(str.isdigit, string or ""))

    # Configurações
    MONGO_CONNECTION_STR = Variable.get("MONGO_CONNECTION_STR_EAF_PRD")
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    if not os_list or not destinatarios:
        raise ValueError("É necessário fornecer parâmetros: os_list e destinatarios.")

    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]

    data_execucao = datetime.datetime.now()
    os.makedirs("reports", exist_ok=True)

    output_csv = os.path.join("reports", f"os_check_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.csv")
    not_found_file = os.path.join("reports", f"os_nao_encontradas_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.txt")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("os_check_manual_detalhado")

    logger.info(f"🔎 Verificando {len(os_list)} OSs...")

    client = MongoClient(MONGO_CONNECTION_STR)
    db = client["eaf"]
    tickets = db["tickets"]

    resultados = []
    nao_encontradas = []

    for os_number in os_list:
        clean_number = only_digits(str(os_number))
        ticket = tickets.find_one({"services.serviceOrder.number": clean_number})

        if ticket:
            # Extrai service e serviceOrder corretos
            service = next((s for s in ticket.get("services", [])
                             if s.get("serviceOrder", {}).get("number") == clean_number), {})
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

    # Grava CSV
    if resultados:
        with open(output_csv, mode="w", newline='', encoding="utf-8-sig") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=list(resultados[0].keys()))
            writer.writeheader()
            writer.writerows(resultados)
        logger.info(f"📂 CSV gerado: {output_csv}")

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
        msg["Subject"] = "CRM > Consulta OS Manual Detalhada"
        msg["From"] = EMAIL_REMETENTE
        msg["To"] = ", ".join(destinatarios)
        msg.set_content(f"""
Prezados,
Segue em anexo o resultado da consulta manual de OS no CRM EAF.
Data de execução: {data_execucao.strftime('%d/%m/%Y %H:%M')}
""")

        # Anexa CSV
        if resultados:
            with open(output_csv, "rb") as f:
                msg.add_attachment(f.read(), maintype="application", subtype="octet-stream", filename=os.path.basename(output_csv))

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


# 2. ✅ Altere o PythonVirtualenvOperator para passar os parâmetros do contexto
os_check_detalhado_task = PythonVirtualenvOperator(
    task_id="os_check_detalhado",
    python_callable=main,
    requirements=["pymongo==4.10.1"],
    system_site_packages=True,
    # 💡 Use op_kwargs para passar os parâmetros do DAG (disponíveis no Jinja template 'dag_run.conf')
    op_kwargs={
        # dag_run.conf contém o JSON que o usuário insere no UI
        "os_list": "{{ dag_run.conf['os_list'] }}",
        "destinatarios": "{{ dag_run.conf['destinatarios'] }}",
    },
    dag=dag,
)

os_check_detalhado_task
