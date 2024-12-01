from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator

def extrair_dados_postgres():
    import psycopg2
    import pandas as pd
    from airflow.models import Variable

    conn_params = {
        'dbname': Variable.get('dbname'),
        'user': Variable.get('user'),
        'password': Variable.get('password'),
        'host': Variable.get('host'),
        'port': Variable.get('port')
    }

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    query = """
    SELECT
        installer."name" AS Instaladora,
        CAST(capacity AS VARCHAR) as Instalacoes_dia,
        ibge.city_code ibge
    FROM public.setup setup
    left join public.installer installer
    on setup.id_installer  = installer.id
    left join public.city_ibge ibge on
    ibge.id = setup.id_city
    """
    cursor.execute(query)
    colunas = [desc[0] for desc in cursor.description]
    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=colunas)
    return df

def extrair_dados_sql_server():
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

    consulta_sql = 'SELECT * FROM [eaf_tvro].[setup_instaladoras]'
    resultado = session.execute(text(consulta_sql))
    setup = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(setup))
    print(setup.head(10))
    return setup

def tratamentos_envio_banco(**kwargs):
    from airflow.models import Variable
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extrair_dados_postgres')
    print(f'tamanho do df:', len(df))
    print(df.head())
    setup = ti.xcom_pull(task_ids='extrair_dados_sql_server')
    print(f'tamanho do setup:', len(setup))
    print(setup.head())
    df = df[~df['concat'].isin(setup['concat'])]
    quantidade_registros = len(df)
    if quantidade_registros > 0:
        df.to_sql("setup_instaladoras", engine, if_exists='append', index=False, schema='eaf_tvro')
    return quantidade_registros
        

def enviar_mensagem(**kwargs):
    import requests
    from airflow.models import Variable
    ti = kwargs['ti']
    quantidade_registros = ti.xcom_pull(task_ids='tratamentos_envio_banco')
    if quantidade_registros > 0:
        chat_id = Variable.get('chat_id')
        token = Variable.get('token_telegram')
        message = f'Foram inseridos {quantidade_registros} novos registros na tabela setup_instaladoras.'
        url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
        requests.get(url)

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
}

dag = DAG(
    'setup_instaladora',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)

enviar_mensagem = PythonOperator(
    task_id='extrairasa_dados',
    python_callable=enviar_mensagem,
    dag=dag
)

extrair_dados_postgres = PythonOperator(
    task_id='extrair_dados_postgres',
    python_callable=extrair_dados_postgres,
    dag=dag
)

extrair_dados_sql_server = PythonOperator(
    task_id='extrair_dados_sql_server',
    python_callable=extrair_dados_sql_server,
    dag=dag
)

tratamentos_envio_banco = PythonOperator(
    task_id='tratamentos_envio_banco',
    python_callable=tratamentos_envio_banco,
    dag=dag
)

extrair_dados_postgres >> extrair_dados_sql_server >> tratamentos_envio_banco >> enviar_mensagem
