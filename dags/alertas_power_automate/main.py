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
        t.IBGE,
        ibge.regiao,
        ibge.Nome_Cidade,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS quantidade_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS quantidade_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE 
    Tipo = 'Service Task' 
    AND Status IN ('2', '7', '8', '10', '11', '12', '13')
    AND StatusdaInstalação NOT IN ('Remarcada Fornecedor', 'Cancelada') AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND CAST(DataHoraAgendamento AS DATE) >= GETDATE() AND CAST(DataHoraAgendamento AS DATE) IS NOT NULL
    GROUP BY
    t.IBGE,
    ibge.regiao,
    ibge.Nome_Cidade
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
        ibge.regiao,
        ibge.Nome_Cidade nome_cidade,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS quantidade_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS quantidade_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE 
    Tipo = 'Service Task' 
    AND Status IN ('2', '7', '8', '10', '11', '12', '13')
    AND StatusdaInstalação NOT IN ('Remarcada Fornecedor', 'Cancelada') AND LOWER(Assunto) NOT LIKE '%zendesk%'
    AND CAST(DataHoraAgendamento AS DATE) < GETDATE() AND CAST(DataHoraAgendamento AS DATE) IS NOT NULL
    GROUP BY
    t.IBGE,
    ibge.regiao,
    ibge.Nome_Cidade
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
        ibge.regiao,
        ibge.Nome_Cidade nome_cidade,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= CAST(DATEADD(DAY, 6, DATEADD(DAY, -16, GETDATE())) AS DATE) THEN 1 ELSE 0 END) AS quantidade_anterior,
        SUM(CASE WHEN CAST(Horadacriação AS DATE) <= GETDATE() THEN 1 ELSE 0 END) AS quantidade_atual
    FROM [eaf_tvro].[ticket_view] t
    LEFT JOIN [eaf_tvro].[ibge]
    ON t.IBGE = ibge.cIBGE
    WHERE StatusdaInstalação = 'Instalada' AND Tipo = 'Service Task' AND Status IN ('4', '5') AND 
    MotivodocontatoInstalação NOT IN ('Cobrança Indevida', 'Contestação', 'Reclamação por problema técnico', 'Manutenção', 'Manutenção - Problema Técnico', 'Manutenção - Técnico Não Compareceu')
    AND LOWER(Assunto) NOT LIKE '%zendesk%'
    GROUP BY
    t.IBGE,
    ibge.regiao,
    ibge.Nome_Cidade
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
        cIBGE ibge
    FROM [eaf_tvro].[ibge]
    '''
    resultado = session.execute(text(consulta_sql))
    todos_ibges = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(todos_ibges))
    print(todos_ibges.head(10))
    return todos_ibges

# junta as informações de ibas e cria o df_final
def cria_df_final(**kwargs):
    ti = kwargs['ti']
    todos_ibges = ti.xcom_pull(task_ids='todos_ibges')
    instalados = ti.xcom_pull(task_ids='instalados')
    backlog = ti.xcom_pull(task_ids='backlog')
    backlog_futuro = ti.xcom_pull(task_ids='backlog_futuro')

    df_final = todos_ibges.merge(instalados, on='ibge', how='left') \
                          .merge(backlog, on='ibge', how='left') \
                          .merge(backlog_futuro, on='ibge', how='left')
    df_final.fillna(0, inplace=True)
    print('colunas:')
    print(df_final.columns)
    print(df_final.head(20))
    return df_final

def calcular_variacao_agendamentos(row):
    if (row['new_agendados_semana_atual'] - row['new_agendados_semana_anterior']) > 20:
        if row['new_agendados_semana_anterior'] == 0:
            return 1
        else:
            return (row['new_agendados_semana_atual'] - row['new_agendados_semana_anterior']) / row['new_agendados_semana_anterior']
    else:
        return 0
    
# função que faz os left join, juntando tudo no df_final
def juntar_tudo_df_final(**kwargs):
    ti = kwargs['ti']
    ibas = ti.xcom_pull(task_ids='cria_df_ibas')
    cadunico = ti.xcom_pull(task_ids='cadunico')
    lista_cidades = ti.xcom_pull(task_ids='lista_cidades')
    df_final = ti.xcom_pull(task_ids='cria_df_final')
    df_final = df_final.merge(ibas, how='left', on='ibge')
    df_final = df_final.merge(cadunico, how='left', on='ibge')
    df_final = df_final.merge(lista_cidades, how='left', on='ibge')

    print(f'tamanho do df {len(df_final)}')
    print(f'todas as colunas {df_final.columns}')
    print('algumas informações')
    print(df_final.head(20))

    return df_final

# junta as informações de iba_semana_anterior e iba_semana_atual
def cria_df_ibas(**kwargs):
    ti = kwargs['ti']
    iba_semana_anterior = ti.xcom_pull(task_ids='iba_semana_anterior')
    iba_semana_atual = ti.xcom_pull(task_ids='iba_semana_atual')
    ibas = iba_semana_anterior.merge(iba_semana_atual, how='left', on='ibge')
    return ibas

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

def cria_coluna_curva(value):
    if value > 9:
        return 'Long Tail'
    elif value > 2:
        return 'Decrescente'
    elif value > 0:
        return 'Crescente'
    return None

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
        Município municipio,
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
        ibge.regiao,
        ibge.Nome_Cidade,
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
    ibge.regiao,
    ibge.Nome_Cidade,
    ibge.domicilios_particulares
    '''
    resultado = session.execute(text(consulta_sql))
    new_agendados_semana_atual = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return new_agendados_semana_atual

# def envio_banco_dados(**kwargs):

#     ti = kwargs['ti']
#     subir = ti.xcom_pull(task_ids='tratando_dados')

#     server = Variable.get('DBSERVER')
#     database = Variable.get('DATABASE')
#     username = Variable.get('DBUSER')
#     password = Variable.get('DBPASSWORD')
#     engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

#     subir.to_sql("capacidade_instaladoras", engine, if_exists='replace', schema='eaf_tvro', index=False)

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

# envio_banco_dados = PythonOperator(
#     task_id='envio_banco_dados',
#     python_callable=envio_banco_dados,
#     dag=dag
# )

# extrair_dados_api = PythonOperator(
#     task_id='extrair_dados_api',
#     python_callable=extrair_dados_api,
#     dag=dag
# ) 








# new_agendados_semana_atual = PythonOperator(
#     task_id='new_agendados_semana_atual',
#     python_callable=new_agendados_semana_atual,
#     dag=dag
# ) 

# new_agendados_semana_anterior = PythonOperator(
#     task_id='new_agendados_semana_anterior',
#     python_callable=new_agendados_semana_anterior,
#     dag=dag,
# ) 

# lista_de_cidades = PythonOperator(
#     task_id='lista_de_cidades',
#     python_callable=lista_de_cidades,
#     dag=dag
# ) 

cria_df_final = PythonOperator(
    task_id='cria_df_final',
    python_callable=cria_df_final,
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

# juntar_tudo_df_final = PythonOperator(
#     task_id='juntar_tudo_df_final',
#     python_callable=juntar_tudo_df_final,
#     dag=dag
# ) 

# cadunico = PythonOperator(
#     task_id='cadunico',
#     python_callable=cadunico,
#     dag=dag
# )

[backlog_futuro, backlog, instalados, todos_ibges] >> cria_df_final
