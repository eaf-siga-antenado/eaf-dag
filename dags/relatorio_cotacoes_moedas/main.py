from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime

with DAG(
    dag_id="cotacao_moeda",
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 11,17 * * *",  # Executa às 11h e às 17h
    catchup=False,
) as dag:

    def tarefa():
        import os
        import requests
        import pandas as pd
        from datetime import datetime, date, timedelta
        import locale
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError

        def get_env_variable(var_name):
            value = os.getenv(var_name)
            if value is None:
                raise EnvironmentError(f"Variável de ambiente '{var_name}' não encontrada.")
            return value

        try:
            server = get_env_variable('DBSERVER')
            database = get_env_variable('DATABASE')
            username = get_env_variable('DBUSER')
            password = get_env_variable('DBPASSWORD')

            engine = create_engine(
                f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            )
        except Exception as e:
            print(f"[ERRO] Falha na configuração de conexão: {e}")
            return

        try:
            url = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,GBP-BRL"
            resposta = requests.get(url, timeout=10)
            resposta.raise_for_status()
            dados = resposta.json()

            cotacao_usd = float(dados["USDBRL"]["bid"])
            cotacao_eur = float(dados["EURBRL"]["bid"])
            cotacao_gbp = float(dados["GBPBRL"]["bid"])

            print(f"Cotação USD: {cotacao_usd}")
            print(f"Cotação EUR: {cotacao_eur}")
            print(f"Cotação GBP: {cotacao_gbp}")
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"[ERRO] Falha ao obter ou processar dados da API: {e}")
            return

        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

        data_atual = date.today()
        ano = data_atual.year
        mes_num = data_atual.month
        mes_abre = data_atual.strftime('%b').capitalize()
        nome_mes = data_atual.strftime('%B').capitalize()
        mes_ano = f"{data_atual.strftime('%b').lower()}/{str(ano)[-2:]}"
        ano_mes_int = int(f"{ano}{mes_num:02d}")
        inicio_mes = datetime(ano, mes_num, 1).strftime('%Y-%m-%d 00:00:00')
        trimestre = (mes_num - 1) // 3 + 1
        bimestre = (mes_num - 1) // 2 + 1
        semestre = 1 if mes_num <= 6 else 2
        semana = data_atual.isocalendar().week
        dia_semana = data_atual.weekday() + 1
        nome_dia = data_atual.strftime("%A").lower()

        if 'feira' not in nome_dia:
            if nome_dia == 'sábado' or nome_dia == 'domingo':
                pass
            else:
                nome_dia += '-feira'

        try:
            with engine.begin() as conn:
                ultima_data = conn.execute(
                    text("SELECT MAX(Data) FROM eaf_tvro.CalendarioCotacao")
                ).scalar()

                if ultima_data is None:
                    ultima_data = date(2025, 1, 1)

                if ultima_data > data_atual:
                    ultima_data = data_atual

                dias_faltantes = pd.date_range(ultima_data + timedelta(days=1), data_atual)

                for data_f in dias_faltantes:
                    data_f = data_f.date()
                    ano = data_f.year
                    mes_num = data_f.month

                    nome_mes = data_f.strftime('%B').capitalize()
                    mes_abre = data_f.strftime('%b').capitalize()
                    mes_ano = f"{data_f.strftime('%b').capitalize()}-{str(ano)[-2:]}"

                    nome_dia = data_f.strftime('%A').lower()
                    if 'feira' not in nome_dia:
                        if nome_dia == 'sábado' or nome_dia == 'domingo':
                            pass
                        else:
                            nome_dia += '-feira'

                    cotacao_existente = conn.execute(
                        text("SELECT CotacaoUSD, CotacaoEUR, CotacaoGBP FROM eaf_tvro.CalendarioCotacao WHERE Data = :data"),
                        {"data": data_f}
                    ).fetchone()

                    if cotacao_existente:
                        if (cotacao_existente[0] != cotacao_usd or cotacao_existente[1] != cotacao_eur or cotacao_existente[2] != cotacao_gbp):
                            conn.execute(
                                text("""
                                    UPDATE eaf_tvro.CalendarioCotacao
                                    SET CotacaoUSD = :usd, CotacaoEUR = :eur, CotacaoGBP = :gbp
                                    WHERE Data = :data
                                """),
                                {
                                    "data": data_f,
                                    "usd": cotacao_usd,
                                    "eur": cotacao_eur,
                                    "gbp": cotacao_gbp
                                }
                            )
                            print(f"[INFO] Cotação atualizada para {data_f}.")
                    else:
                        conn.execute(
                            text(""" 
                                INSERT INTO eaf_tvro.CalendarioCotacao (
                                    Data, Ano, NomeMes, MesAbre, MesAno, MesNum,
                                    AnoMesINT, InicioMes, Trimestre, TrimestreAbreviado,
                                    Bimestre, Semestre, Semana, DiaSemana, NomeDia,
                                    CotacaoUSD, CotacaoEUR, CotacaoGBP
                                )
                                VALUES (
                                    :data, :ano, :nomemes, :mesabre, :mesano, :mesnum,
                                    :anomesint, :iniciomes, :trimestre, :trimestreabrev,
                                    :bimestre, :semestre, :semana, :diasemana, :nomedia,
                                    :usd, :eur, :gbp
                                )
                            """),
                            {
                                "data": data_f,
                                "ano": ano,
                                "nomemes": nome_mes,
                                "mesabre": mes_abre,
                                "mesano": mes_ano,
                                "mesnum": mes_num,
                                "anomesint": ano_mes_int,
                                "iniciomes": inicio_mes,
                                "trimestre": trimestre,
                                "trimestreabrev": f"{trimestre}º Trim",
                                "bimestre": f"{bimestre}º Bim",
                                "semestre": f"{semestre}º Sem",
                                "semana": semana,
                                "diasemana": dia_semana,
                                "nomedia": nome_dia,
                                "usd": cotacao_usd,
                                "eur": cotacao_eur,
                                "gbp": cotacao_gbp
                            }
                        )
                        print(f"[INFO] Novo registro inserido para {data_f}.")
        except SQLAlchemyError as e:
            print(f"[ERRO] Falha ao executar operação no banco de dados: {e}")

    PythonVirtualenvOperator(
        task_id="cotacao_diaria_moeda",
        python_callable=tarefa,
        requirements=[
            "pandas",
            "requests",
            "sqlalchemy",
            "pyodbc"
        ],
        system_site_packages=True
    )
