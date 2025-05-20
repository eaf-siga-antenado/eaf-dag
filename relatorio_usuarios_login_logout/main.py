import os
import csv
from pymongo import MongoClient
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta

EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_EMAIL = os.getenv("SENHA_EMAIL")


def enviar_email_com_csv(
    caminho_csv,
    destinatarios,
    assunto="Relatório CSV",
    corpo="Segue em anexo o relatório.",
):
    try:
        # Criar a mensagem de e-mail
        msg = EmailMessage()
        msg["Subject"] = assunto
        msg["From"] = EMAIL_REMETENTE
        msg["To"] = ", ".join(destinatarios)
        msg.set_content(corpo)

        # Anexar o CSV
        with open(caminho_csv, "rb") as f:
            conteudo = f.read()
            nome_arquivo = os.path.basename(caminho_csv)
            msg.add_attachment(
                conteudo,
                maintype="application",
                subtype="octet-stream",
                filename=nome_arquivo,
            )

        # Enviar e-mail via SMTP (Gmail como exemplo)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
            smtp.send_message(msg)

        print("✅ E-mail enviado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")


def gerar_relatorio_login_logout(output_file, data):
    MONGO_CONNECTION_STR = os.getenv("MONGO_CONNECTION_STR_PRD")

    # Definir a data como o dia anterior à execução (1h da manhã)
    dia_seguinte = data + timedelta(days=1)

    client = MongoClient(MONGO_CONNECTION_STR)
    db = client["eaf"]
    collection = db["userLoginTimeTracking"]

    # Buscar todos os eventos do dia anterior
    query = {
        "loginTime": {
            "$gte": datetime.combine(data, datetime.min.time()),
            "$lt": datetime.combine(dia_seguinte, datetime.min.time()),
        }
    }
    print(f"Query executada: {query}")
    print(f"Buscando eventos de login/logout do dia: {data}")

    eventos = list(collection.find(query).sort([("username", 1), ("loginTime", 1)]))

    # Agrupar por usuário
    eventos_por_usuario = {}
    for evento in eventos:
        username = evento.get("username", "NULL")
        if username not in eventos_por_usuario:
            eventos_por_usuario[username] = []
        eventos_por_usuario[username].append(evento)

    # Preparar CSV
    os.makedirs("reports", exist_ok=True)
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(["usuario", "data", "horario entrada", "horario saida"])

        for username, eventos in eventos_por_usuario.items():
            i = 0
            while i < len(eventos):
                evento = eventos[i]
                if evento["event"] == "LOGIN":
                    entrada = evento["loginTime"]
                    saida = None

                    # Procurar próximo LOGOUT
                    for j in range(i + 1, len(eventos)):
                        if eventos[j]["event"] == "LOGOUT":
                            saida = eventos[j]["loginTime"]
                            i = j  # pular até o logout
                            break

                    writer.writerow(
                        [
                            username,
                            entrada.strftime("%Y-%m-%d"),
                            entrada.strftime("%H:%M:%S"),
                            saida.strftime("%H:%M:%S") if saida else "N/A",
                        ]
                    )
                i += 1

    client.close()
    print(f"\u2705 Relatório salvo em: {output_file}")
    return output_file


if __name__ == "__main__":
    data = (datetime.now() - timedelta(days=1)).date()
    output_file = f"reports/relatorio_login_logout_{data.strftime('%d-%m-%Y')}.csv"
    gerar_relatorio_login_logout(output_file, data)
    enviar_email_com_csv(
        output_file,
        [
            "marcelo.ferreira.terceirizado@eaf.org.br",
            "felipe.silva.terceirizado@eaf.org.br",
        ],
    )
