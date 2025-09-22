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
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
        from airflow.models import Variable

        def get_env_variable(var_name):
            value = Variable.get(var_name)
            if value is None:
                raise EnvironmentError(f"Variável de ambiente '{var_name}' não encontrada.")
            return value

        meses_pt = {
            'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
            'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
            'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
            'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
        }
        meses_abrev_pt = {
            'Jan': 'Jan', 'Feb': 'Fev', 'Mar': 'Mar', 'Apr': 'Abr',
            'May': 'Mai', 'Jun': 'Jun', 'Jul': 'Jul', 'Aug': 'Ago',
            'Sep': 'Set', 'Oct': 'Out', 'Nov': 'Nov', 'Dec': 'Dez'
        }
        dias_pt = {
            'monday': 'segunda-feira',
            'tuesday': 'terça-feira',
            'wednesday': 'quarta-feira',
            'thursday': 'quinta-feira',
            'friday': 'sexta-feira',
            'saturday': 'sábado',
            'sunday': 'domingo'
        }

        def traduzir_data(data_ref):
            """Recebe um objeto date e retorna os campos traduzidos em PT-BR."""
            ano = data_ref.year
            mes_num = data_ref.month
            nome_mes = meses_pt[data_ref.strftime('%B')]
            mes_abre = meses_abrev_pt[data_ref.strftime('%b')]
            mes_ano = f"{mes_abre.lower()}/{str(ano)[-2:]}"
            ano_mes_int = int(f"{ano}{mes_num:02d}")
            inicio_mes = datetime(ano, mes_num, 1).strftime('%Y-%m-%d 00:00:00')
            trimestre = (mes_num - 1) // 3 + 1
            bimestre = (mes_num - 1) // 2 + 1
            semestre = 1 if mes_num <= 6 else 2
            semana = data_ref.isocalendar()[1]
            dia_semana = data_ref.weekday() + 1
            nome_dia = dias_pt[data_ref.strftime("%A").lower()]
            if 'feira' not in nome_dia and nome_dia not in ['sábado', 'domingo']:
                nome_dia += '-feira'
            return {
                "ano": ano,
                "mes_num": mes_num,
                "nome_mes": nome_mes,
                "mes_abre": mes_abre,
                "mes_ano": mes_ano,
                "ano_mes_int": ano_mes_int,
                "inicio_mes": inicio_mes,
                "trimestre": trimestre,
                "bimestre": bimestre,
                "semestre": semestre,
                "semana": semana,
                "dia_semana": dia_semana,
                "nome_dia": nome_dia
            }

        try:
            server = get_env_variable('DBSERVER')
            database = get_env_variable('DATABASE')
            username = get_env_variable('DBUSER')
            password = get_env_variable('DBPASSWORD')
            engine = create_engine(
                f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+18+for+SQL+Server"
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
        except Exception as e:
            print(f"[ERRO] Falha ao obter dados da API: {e}")
            return

        hoje = date.today()
        dados_hoje = traduzir_data(hoje)
        print(f"[DEBUG] {dados_hoje}")

        try:
            with engine.begin() as conn:
                ultima_data = conn.execute(
                    text("SELECT MAX(Data) FROM eaf_tvro.CalendarioCotacao")
                ).scalar()

                if ultima_data is None:
                    ultima_data = date(2025, 1, 1)

                if ultima_data > hoje:
                    ultima_data = hoje

                dias_faltantes = pd.date_range(ultima_data + timedelta(days=1), hoje)

                for data_f in dias_faltantes:
                    data_f = data_f.date()
                    dados_data = traduzir_data(data_f)

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
                                {"data": data_f, "usd": cotacao_usd, "eur": cotacao_eur, "gbp": cotacao_gbp}
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
                                "ano": dados_data["ano"],
                                "nomemes": dados_data["nome_mes"],
                                "mesabre": dados_data["mes_abre"],
                                "mesano": dados_data["mes_ano"],
                                "mesnum": dados_data["mes_num"],
                                "anomesint": dados_data["ano_mes_int"],
                                "iniciomes": dados_data["inicio_mes"],
                                "trimestre": dados_data["trimestre"],
                                "trimestreabrev": f"{dados_data['trimestre']}º Trim",
                                "bimestre": f"{dados_data['bimestre']}º Bim",
                                "semestre": f"{dados_data['semestre']}º Sem",
                                "semana": dados_data["semana"],
                                "diasemana": dados_data["dia_semana"],
                                "nomedia": dados_data["nome_dia"],
                                "usd": cotacao_usd,
                                "eur": cotacao_eur,
                                "gbp": cotacao_gbp
                            }
                        )
                        print(f"[INFO] Novo registro inserido para {data_f}.")
        except SQLAlchemyError as e:
            print(f"[ERRO] Falha no banco: {e}")

    PythonVirtualenvOperator(
        task_id="cotacao_diaria_moeda",
        python_callable=tarefa,
        requirements=["pandas", "requests", "sqlalchemy", "pyodbc"],
        system_site_packages=True
    )

