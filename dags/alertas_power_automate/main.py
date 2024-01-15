import csv
import requests
import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def backlog_futuro():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        t.IBGE ibge,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS backlog_futuro_semana_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS backlog_futuro_semana_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE 
    Tipo = 'Service Task' 
    AND Status IN ('2', '7', '8', '10', '11', '12', '13')
    AND StatusdaInstalação NOT IN ('Remarcada Fornecedor', 'Cancelada') AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND CAST(DataHoraAgendamento AS DATE) >= GETDATE() AND CAST(DataHoraAgendamento AS DATE) IS NOT NULL
    GROUP BY
    t.IBGE
    '''
    resultado = session.execute(text(consulta_sql))
    backlog_futuro = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(backlog_futuro))
    print(backlog_futuro.head(10))
    return backlog_futuro

def backlog():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        t.IBGE ibge,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS backlog_semana_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS backlog_semana_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE 
    Tipo = 'Service Task' 
    AND Status IN ('2', '7', '8', '10', '11', '12', '13')
    AND StatusdaInstalação NOT IN ('Remarcada Fornecedor', 'Cancelada') AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND CAST(DataHoraAgendamento AS DATE) < GETDATE() AND CAST(DataHoraAgendamento AS DATE) IS NOT NULL
    GROUP BY
    t.IBGE
    '''
    resultado = session.execute(text(consulta_sql))
    backlog = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(backlog))
    print(backlog.head(10))
    return backlog

def instalados():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        t.IBGE ibge,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS instalados_semana_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS instalados_semana_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE StatusdaInstalação = 'Instalada' AND Tipo = 'Service Task' AND Status IN ('4', '5') AND 
    MotivodocontatoInstalação NOT IN ('Cobrança Indevida', 'Contestação', 'Reclamação por problema técnico', 'Manutenção', 'Manutenção - Problema Técnico', 'Manutenção - Técnico Não Compareceu')
    AND LOWER(Assunto) NOT LIKE '%zendesk%'
    GROUP BY
    t.IBGE
    '''
    resultado = session.execute(text(consulta_sql))
    instalados = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(instalados))
    print(instalados.head(10))
    return instalados

def todos_ibges():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        cIBGE ibge,
        regiao,
        nome_cidade
    FROM [eaf_tvro].[ibge]
    '''
    resultado = session.execute(text(consulta_sql))
    todos_ibges = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(todos_ibges))
    print(todos_ibges.head(10))
    return todos_ibges

# junta as informações de ibas e cria o cria_df_ibas
def cria_df_ibas(**kwargs):
    ti = kwargs['ti']
    todos_ibges = ti.xcom_pull(task_ids='todos_ibges')
    instalados = ti.xcom_pull(task_ids='instalados')
    backlog = ti.xcom_pull(task_ids='backlog')
    backlog_futuro = ti.xcom_pull(task_ids='backlog_futuro')

    df_ibas = todos_ibges.merge(instalados, on='ibge', how='left') \
                          .merge(backlog, on='ibge', how='left') \
                          .merge(backlog_futuro, on='ibge', how='left')
    df_ibas.fillna(0, inplace=True)
    df_ibas['iba_semana_anterior'] = df_ibas['instalados_semana_anterior'] + df_ibas['backlog_semana_anterior'] + df_ibas['backlog_futuro_semana_anterior']
    df_ibas['iba_semana_atual'] = df_ibas['instalados_semana_atual'] + df_ibas['backlog_semana_atual'] + df_ibas['backlog_futuro_semana_atual']
    df_ibas.drop(columns=['instalados_semana_anterior', 'backlog_semana_anterior', 'backlog_futuro_semana_anterior', 'instalados_semana_atual', 'backlog_semana_atual', 'backlog_futuro_semana_atual'], inplace=True)
    return df_ibas

# função que faz os left join, juntando tudo no df_final
def criar_df_final(**kwargs):
    ti = kwargs['ti']
    ibas = ti.xcom_pull(task_ids='cria_df_ibas')
    cadunico = ti.xcom_pull(task_ids='cadunico')
    lista_de_cidades = ti.xcom_pull(task_ids='lista_de_cidades')
    new_agendados_semana_anterior = ti.xcom_pull(task_ids='new_agendados_semana_anterior')
    new_agendados_semana_atual = ti.xcom_pull(task_ids='new_agendados_semana_atual')
    df_final = ibas.merge(cadunico, how='left', on='ibge')\
                   .merge(lista_de_cidades, how='left', on='ibge')\
                   .merge(new_agendados_semana_anterior, how='left', on='ibge')\
                   .merge(new_agendados_semana_atual, how='left', on='ibge')

    print(f'tamanho do df {len(df_final)}')
    print(f'todas as colunas {df_final.columns}')
    print('algumas informações')
    print(df_final.head(20))
    return df_final

def cadunico():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT 
        CAST(ibge AS varchar) ibge,
        familias qtd_cadunico
    FROM [eaf_tvro].[cadunico]
    '''
    resultado = session.execute(text(consulta_sql))
    cadunico = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return cadunico

def lista_de_cidades():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT 
        [cód. IBGE] ibge,
        FASE fase,
        [INÍCIO DE CAMPANHA] inicio_campanha,
        DATEDIFF(MONTH, [INÍCIO DE CAMPANHA], GETDATE()) diferenca_em_meses
    FROM eaf_tvro.lista_cidades
    '''
    resultado = session.execute(text(consulta_sql))
    lista_de_cidades = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    lista_de_cidades['diferenca_em_meses'] = lista_de_cidades['diferenca_em_meses'].fillna(0)
    lista_de_cidades['diferenca_em_meses'] = lista_de_cidades['diferenca_em_meses'].astype(int)

    # criando a coluna de curva
    lista_de_cidades['curva'] = lista_de_cidades['diferenca_em_meses'].apply(cria_coluna_curva)

    # removendo registros onde a curva é nula
    lista_de_cidades = lista_de_cidades[~lista_de_cidades['curva'].isna()]
    lista_de_cidades['ibge'] = lista_de_cidades['ibge'].astype('str')
    return lista_de_cidades

def new_agendados_semana_anterior():
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        t.IBGE ibge,
        COUNT(t.IDdoticket) new_agendados_semana_anterior
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE (CAST(Horadacriação AS DATE) >= CAST(DATEADD(DAY, -16, GETDATE()) AS DATE) AND CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE))
    AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND Status IN ('2', '7', '8', '10', '11', '12', '13') AND DataHoraAgendamento IS NOT NULL AND StatusdaInstalação <> 'Remarcada Fornecedor' AND t.IBGE <> ''
    GROUP BY
    t.IBGE
    '''
    resultado = session.execute(text(consulta_sql))
    new_agendados_semana_anterior = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return new_agendados_semana_anterior

def new_agendados_semana_atual():

    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = '''
    SELECT
        t.IBGE ibge,
        ibge.domicilios_particulares qtd_domicilios,
        COUNT(t.IDdoticket) new_agendados_semana_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE (CAST(Horadacriação AS DATE) >= CAST(DATEADD(DAY, -9, GETDATE()) AS DATE) AND CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -9, GETDATE())) AS DATE))
    AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND Status IN ('2', '7', '8', '10', '11', '12', '13') AND DataHoraAgendamento IS NOT NULL AND StatusdaInstalação <> 'Remarcada Fornecedor' AND t.IBGE IS NOT NULL
    GROUP BY
    t.IBGE,
    ibge.domicilios_particulares
    '''
    resultado = session.execute(text(consulta_sql))
    new_agendados_semana_atual = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return new_agendados_semana_atual

# cálculo variação de agendamentos
def calcular_variacao_agendamentos(row):
    if (row['new_agendados_semana_atual'] - row['new_agendados_semana_anterior']) > 20:
        if row['new_agendados_semana_anterior'] == 0:
            return 1
        else:
            return (row['new_agendados_semana_atual'] - row['new_agendados_semana_anterior']) / row['new_agendados_semana_anterior']
    else:
        return 0

def cria_coluna_curva(value):
    if value > 9:
        return 'Long Tail'
    elif value > 2:
        return 'Decrescente'
    elif value > 0:
        return 'Crescente'
    return None

# porcentagem_iba_cadunico_semana_anterior 
def porcentagem_iba_cadunico_semana_anterior(row):
    return row['iba_semana_anterior'] / row['qtd_cadunico'] if row['qtd_cadunico'] else 0

# porcentagem_iba_cadunico_semana_atual 
def porcentagem_iba_cadunico_semana_atual(row):
    return row['iba_semana_atual'] / row['qtd_cadunico'] if row['qtd_cadunico'] else 0

# porcentagem iba domicilios semana anterior
def porcentagem_iba_domicilios_semana_anterior(row):
    return (row['iba_semana_anterior'] / row['qtd_domicilios']) if row['qtd_domicilios'] else 0

# porcentagem iba domicilios semana atual
def porcentagem_iba_domicilios_semana_atual(row):
    return (row['iba_semana_atual'] / row['qtd_domicilios']) if row['qtd_domicilios'] else 0

# % de risco semana anterior
def porcentagem_de_risco_semana_anterior(row):
    domicilios = row['porcentagem_iba_domicilios_semana_anterior'] * 0.5
    codfamilia = row['porcentagem_iba_cadunico_semana_anterior'] * 0.5
    resultado = (domicilios + codfamilia) / 1
    return resultado if resultado else 0

# % de risco semana atual
def porcentagem_de_risco_semana_atual(row):
    domicilios = row['porcentagem_iba_domicilios_semana_atual'] * 0.5
    codfamilia = row['porcentagem_iba_cadunico_semana_atual'] * 0.5
    resultado = (domicilios + codfamilia) / 1
    return resultado if resultado else 0

# alerta de prevenção crescente
def alerta_prevencao_crescente(row):
    if not row['variacao_agendamentos_semana']:
        return None
    elif row['variacao_agendamentos_semana'] > 0.4 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.5:
        return 'Comunicado - Verde'
    elif row['variacao_agendamentos_semana'] > 0.4 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Alerta - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.4 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Comunicado - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.4 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho'
    elif row['variacao_agendamentos_semana'] > 0.4 and row['porcentagem_de_risco_semana_anterior'] > 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho Crítico'
    
# alerta de prevenção decrescente
def alerta_prevencao_decrescente(row):
    if not row['variacao_agendamentos_semana']:
        return None
    elif row['variacao_agendamentos_semana'] > 0.2 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.5:
        return 'Comunicado - Verde'
    elif row['variacao_agendamentos_semana'] > 0.2 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Alerta - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.2 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Comunicado - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.2 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho'
    elif row['variacao_agendamentos_semana'] > 0.2 and row['porcentagem_de_risco_semana_anterior'] > 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho Crítico' 

# alerta de prevenção longtail
def alerta_prevencao_longtail(row):
    if not row['variacao_agendamentos_semana']:
        return None
    elif row['variacao_agendamentos_semana'] > 0.3 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.5:
        return 'Comunicado - Verde'
    elif row['variacao_agendamentos_semana'] > 0.3 and row['porcentagem_de_risco_semana_anterior'] < 0.5 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Alerta - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.3 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] < 0.8:
        return 'Comunicado - Amarelo'
    elif row['variacao_agendamentos_semana'] > 0.3 and row['porcentagem_de_risco_semana_anterior'] < 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho'
    elif row['variacao_agendamentos_semana'] > 0.3 and row['porcentagem_de_risco_semana_anterior'] > 0.8 and row['porcentagem_de_risco_semana_atual'] > 0.8:
        return 'Alerta - Vermelho Crítico'
    
# alerta cálculo prevenção
def calculo_prevencao_funcao(row):
    if row['curva'] == 'Crescente':
        return row['alerta_prevencao_crescente']
    elif row['curva'] == 'Long Tail':
        return row['alerta_prevencao_longtail']
    elif row['curva'] == 'Descrescente':
        return row['alerta_prevencao_decrescente']
    return None

# criação de nível de acordo com a informação calculo_prevencao
def nivel_prevencao_funcao(row):
    if row['calculo_prevencao'] == 'Comunicado - Verde':
        return 1
    elif row['calculo_prevencao'] == 'Alerta - Amarelo':
        return 2
    elif row['calculo_prevencao'] == 'Comunicado - Amarelo':
        return 3
    elif row['calculo_prevencao'] == 'Alerta - Vermelho':
        return 4
    elif row['calculo_prevencao'] == 'Alerta - Vermelho Crítico':
        return 5
    return 0

# criando colunas calculadas
def cria_colunas_calculadas(**kwargs):
    ti = kwargs['ti']
    df_final = ti.xcom_pull(task_ids='criar_df_final')
    df_final['variacao_agendamentos_semana'] = df_final.apply(calcular_variacao_agendamentos, axis=1)
    df_final['porcentagem_iba_cadunico_semana_anterior'] = df_final.apply(porcentagem_iba_cadunico_semana_anterior, axis=1)
    df_final['porcentagem_iba_cadunico_semana_atual'] = df_final.apply(porcentagem_iba_cadunico_semana_atual, axis=1)
    df_final['porcentagem_iba_domicilios_semana_anterior'] = df_final.apply(porcentagem_iba_domicilios_semana_anterior, axis=1)
    df_final['porcentagem_iba_domicilios_semana_atual'] = df_final.apply(porcentagem_iba_domicilios_semana_atual, axis=1)
    df_final['porcentagem_de_risco_semana_anterior'] = df_final.apply(porcentagem_de_risco_semana_anterior, axis=1)
    df_final['porcentagem_de_risco_semana_atual'] = df_final.apply(porcentagem_de_risco_semana_atual, axis=1)
    df_final['alerta_prevencao_crescente'] = df_final.apply(alerta_prevencao_crescente, axis=1)
    df_final['alerta_prevencao_decrescente'] = df_final.apply(alerta_prevencao_decrescente, axis=1)
    df_final['alerta_prevencao_longtail'] = df_final.apply(alerta_prevencao_longtail, axis=1)
    df_final['calculo_prevencao'] = df_final.apply(calculo_prevencao_funcao, axis=1)
    df_final['nivel_calculo_prevencao'] = df_final.apply(nivel_prevencao_funcao, axis=1)
    df_final = df_final[df_final['nivel_calculo_prevencao'] >= 2]
    return df_final

def cidades_alertadas_pa(**kwargs):
    import pyodbc
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text
    ti = kwargs['ti']
    df_final = ti.xcom_pull(task_ids='cria_colunas_calculadas')
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    Session = sessionmaker(bind=engine)
    session = Session()
    consulta_sql = ' SELECT * FROM eaf_tvro.cidades_alertadas_pa'
    resultado = session.execute(text(consulta_sql))
    cidades_alertadas_pa = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    if len(cidades_alertadas_pa) > 0:
        for _, row in df_final.iterrows():
            ibge = row['ibge']
            calculo_prevensao = row['calculo_prevensao']
            if((cidades_alertadas_pa['ibge'] == ibge) & (cidades_alertadas_pa['calculo_prevensao'] == calculo_prevensao)).any():
                pass
            else:
                ibge = row['ibge'], 
                nome_cidade = row['nome_cidade'], 
                calculo_prevencao = row['calculo_prevencao'], 
                nivel_alerta = row['nivel_alerta'], 
                data_alerta = row['data_alerta']
                server = 'sqlserver-eaf.database.windows.net'
                database = 'database-middleware'
                username = 'eaf_svc'
                password = 'etzAf*!Hk4WX'

                conn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
                cursor = conn.cursor()
                insere_informacao = f"INSERT INTO [eaf_tvro].[cidades_alertadas_pa] (ibge, nome_cidade, calculo_prevencao, nivel_alerta, data_alerta) VALUES ({ibge}, {nome_cidade}, {calculo_prevencao}, {nivel_alerta}, {data_alerta})"
                cursor.execute(insere_informacao)
                cursor.commit()
                ibge = row['ibge']
                regiao = row['regiao']
                nome_cidade = row['nome_cidade']
                agendados_semana_anterior = row['agendados_semana_anterior']
                agendados_semana_atual = row['agendados_semana_atual']
                variacao_agendamentos_semana = row['variacao_agendamentos_semana']
                risco_semana_anterior = row['risco_semana_anterior']
                risco_semana_atual = row['risco_semana_atual']
                curva = row['curva']
                calculo_prevencao = row['calculo_prevencao']
                insere_informacao = f"INSERT INTO [eaf_tvro].[disparo_alerta_pa] (ibge, regiao, nome_cidade, agendados_semana_anterior, agendados_semana_atual, variacao_agendamentos_semana, risco_semana_anterior, risco_semana_atual, curva, calculo_prevencao) VALUES ({ibge}, {regiao}, {nome_cidade}, {agendados_semana_anterior}, {agendados_semana_atual}, {variacao_agendamentos_semana}, {risco_semana_anterior}, {risco_semana_atual}, {curva}, {calculo_prevencao})"
                cursor.execute(insere_informacao)
                cursor.commit()

    print('DEU CERTO!!!')

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'alertas_power_automate',
    default_args=default_args,
    schedule_interval='20 11 * * *',
    catchup=False
)

new_agendados_semana_atual = PythonOperator(
    task_id='new_agendados_semana_atual',
    python_callable=new_agendados_semana_atual,
    dag=dag
) 

new_agendados_semana_anterior = PythonOperator(
    task_id='new_agendados_semana_anterior',
    python_callable=new_agendados_semana_anterior,
    dag=dag,
) 

lista_de_cidades = PythonOperator(
    task_id='lista_de_cidades',
    python_callable=lista_de_cidades,
    dag=dag
) 

cria_df_ibas = PythonOperator(
    task_id='cria_df_ibas',
    python_callable=cria_df_ibas,
    dag=dag
) 

backlog_futuro = PythonOperator(
    task_id='backlog_futuro',
    python_callable=backlog_futuro,
    dag=dag
)

backlog = PythonOperator(
    task_id='backlog',
    python_callable=backlog,
    dag=dag
)

instalados = PythonOperator(
    task_id='instalados',
    python_callable=instalados,
    dag=dag
)

todos_ibges = PythonOperator(
    task_id='todos_ibges',
    python_callable=todos_ibges,
    dag=dag
)

criar_df_final = PythonOperator(
    task_id='criar_df_final',
    python_callable=criar_df_final,
    dag=dag
) 

cadunico = PythonOperator(
    task_id='cadunico',
    python_callable=cadunico,
    dag=dag
)

cria_colunas_calculadas = PythonOperator(
    task_id='cria_colunas_calculadas',
    python_callable=cria_colunas_calculadas,
    dag=dag
)

[backlog_futuro, backlog, instalados, todos_ibges] >> cria_df_ibas

cria_df_ibas >> [lista_de_cidades, cadunico, new_agendados_semana_atual, new_agendados_semana_anterior] >> criar_df_final >> cria_colunas_calculadas
