import os
import csv
from pymongo import MongoClient
from datetime import datetime, timedelta

def gerar_relatorio_login_logout():
    MONGO_CONNECTION_STR = os.getenv("MONGO_CONNECTION_STR_PRD")

    # Definir a data como o dia anterior à execução (1h da manhã)
    data = (datetime.now() - timedelta(days=1)).date()
    dia_seguinte = data + timedelta(days=1)

    client = MongoClient(MONGO_CONNECTION_STR)
    db = client["eaf"]
    collection = db["userLoginTimeTracking"]

    # Buscar todos os eventos do dia anterior
    query = {
        "loginTime": {"$gte": datetime.combine(data, datetime.min.time()), "$lt": datetime.combine(dia_seguinte, datetime.min.time())}
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
    output_file = f"reports/relatorio_login_logout_{data.strftime('%Y-%m-%d')}.csv"
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

                    writer.writerow([
                        username,
                        entrada.strftime("%Y-%m-%d"),
                        entrada.strftime("%H:%M:%S"),
                        saida.strftime("%H:%M:%S") if saida else "N/A"
                    ])
                i += 1

    client.close()
    print(f"\u2705 Relatório salvo em: {output_file}")
    return output_file

if __name__ == "__main__":
    gerar_relatorio_login_logout()
