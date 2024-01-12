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

def lista_cidades():
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
    lista_cidades = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    lista_cidades.head()

    # print(len(lista_cidades))
    # print(lista_cidades.head(10))

    return lista_cidades

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
    new_agendados_semana_anterior.head()

    # print(len(new_agendados_semana_anterior))
    # print(new_agendados_semana_anterior.head(10))

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
    new_agendados_semana_atual.head()

    # print(len(new_agendados_semana_atual))
    # print(new_agendados_semana_atual.head(10))

    return new_agendados_semana_atual

def imprimir_informacao(**kwargs):
    ti = kwargs['ti']
    new_agendados_semana_atual = ti.xcom_pull(task_ids='new_agendados_semana_atual')
    new_agendados_semana_anterior = ti.xcom_pull(task_ids='new_agendados_semana_anterior')
    lista_cidades = ti.xcom_pull(task_ids='lista_cidades')
    print('new_agendados_semana_atual')
    print(len(new_agendados_semana_atual))
    print(new_agendados_semana_atual.head())
    print('new_agendados_semana_anterior')
    print(len(new_agendados_semana_anterior))
    print(new_agendados_semana_anterior.head())
    print('lista_cidades')
    print(len(lista_cidades))
    print(lista_cidades.head())

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
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'alertas_power_automate',
    default_args=default_args,
    schedule_interval=None,
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

imprimir_informacao = PythonOperator(
    task_id='imprimir_informacao',
    python_callable=imprimir_informacao,
    dag=dag
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

lista_cidades = PythonOperator(
    task_id='lista_cidades',
    python_callable=lista_cidades,
    dag=dag,
    trigger_rule='one_success'
) 

[new_agendados_semana_atual, new_agendados_semana_anterior, lista_cidades] >> imprimir_informacao
