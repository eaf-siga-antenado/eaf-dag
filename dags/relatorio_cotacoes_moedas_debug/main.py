from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
from airflow.models import Variable

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ============================
# Configurações padrão do DAG
# ============================
default_args = {
    "owner": "airflow",
    "retries": 0,  # MUITO IMPORTANTE → evita 429
}


# ============================
# Função principal
# ============================
def cotacao_diaria():
    # ----------------------------
    # Conexão com banco
    # ----------------------------
    engine = create_engine(
        f"mssql+pyodbc://{Variable.get('DBUSER')}:"
        f"{Variable.get('DBPASSWORD')}@"
        f"{Variable.get('DBSERVER')}:1433/"
        f"{Variable.get('DATABASE')}?"
        "driver=ODBC+Driver+18+for+SQL+Server"
    )

    hoje = date.today()

    # ----------------------------
    # Verifica se já existe cotação hoje
    # ----------------------------
    with engine.begin() as conn:
        existe = conn.execute(
            text("""
                SELECT 1
                FROM eaf_tvro.CalendarioCotacao
                WHERE Data = :data
            """),
            {"data": hoje}
        ).fetchone()

        if existe:
            print(f"[INFO] Cotação já existente para {hoje}. Execução encerrada.")
            return

    # ----------------------------
    # Chamada da API (1x)
    # ----------------------------
    url = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,GBP-BRL"
    response = requests.get(url, timeout=10)

    # Proteção explícita contra 429
    if response.status_code == 429:
        print("[WARN] API retornou 429. Execução encerrada sem retry.")
        return

    response.raise_for_status()
    dados = response.json()

    cotacao_usd = float(dados["USDBRL"]["bid"])
    cotacao_eur = float(dados["EURBRL"]["bid"])
    cotacao_gbp = float(dados["GBPBRL"]["bid"])

    agora = datetime.utcnow()

    # ----------------------------
    # Inserção da cotação do dia
    # ----------------------------
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO eaf_tvro.CalendarioCotacao (
                        Data,
                        CotacaoUSD,
                        CotacaoEUR,
                        CotacaoGBP,
                        DataAtualizacao
                    )
                    VALUES (
                        :data,
                        :usd,
                        :eur,
                        :gbp,
                        :atualizado_em
                    )
                """),
                {
                    "data": hoje,
                    "usd": cotacao_usd,
                    "eur": cotacao_eur,
                    "gbp": cotacao_gbp,
                    "atualizado_em": agora
                }
            )

            print(f"[INFO] Cotação inserida com sucesso para {hoje}.")

    except SQLAlchemyError as e:
        print(f"[ERRO] Falha ao salvar cotação: {e}")
        raise


# ============================
# Definição do DAG
# ============================
with DAG(
    dag_id="cotacao_moeda_diaria_debug",
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 11,17 * * *",  # 2x por dia
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["financeiro", "cotacao", "api"]
) as dag:

    PythonOperator(
        task_id="salvar_cotacao_diaria",
        python_callable=cotacao_diaria
    )