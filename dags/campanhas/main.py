import requests
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, NVARCHAR
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator


def extrair_dados_api():
    url_base = "https://api-eaf.azurewebsites.net/tracking/campaigns"
    headers = {
        "accept": "*/*",
        'Authorization': 's&dsdsa@123iudhasdiahsgd#@!'
    }
    params = {
        # "startDate": "2025-08-01",
        "startDate": "2026-01-20",
        "endDate": "2026-01-31",
        "take": 5000
    }
    skip = 0
    df_final = pd.DataFrame()
    while True:
        params["skip"] = skip
        response = requests.get(url_base, headers=headers, params=params)
        if response.status_code != 200:
            break
        data = response.json()
        if not data or len(data['data']) == 0:
            break
        df = pd.DataFrame(data)
        df_final = pd.concat([df_final, df], ignore_index=True)
        skip += params["take"]
    return df_final.to_dict()


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
    consulta_sql = """
        SELECT
            cIBGE ibge,
            nome_cidade
        FROM [eaf_tvro].[ibge]
        WHERE cIBGE in (
        SELECT
            cIBGE ibge
        FROM [eaf_tvro].[ibge_fase_extra]
        )
    """
    resultado = session.execute(text(consulta_sql))
    ibge = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return ibge.to_dict()


def tratamentos_envio_banco(df_completo_dict, ibge_dict):
    import pandas as pd
    from airflow.models import Variable
    from rapidfuzz import process, fuzz
    from sqlalchemy import create_engine, NVARCHAR
    
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    df_completo = pd.DataFrame(df_completo_dict)
    ibge = pd.DataFrame(ibge_dict)

    df_completo = pd.DataFrame(df_completo['data'].tolist())

    cidades_oficiais = ibge['nome_cidade'].tolist()

    def normalizar_cidade(cidade):
        if pd.isna(cidade):
            return None, None
        
        resultado = process.extractOne(
            cidade, 
            cidades_oficiais,
            scorer=fuzz.token_sort_ratio
        )
        
        if resultado:
            return resultado[0], resultado[1]
        return cidade, 0

    cidades_unicas = df_completo['city'].unique()
    mapeamento = {}
    for cidade in cidades_unicas:
        nome_normalizado, score = normalizar_cidade(cidade)
        mapeamento[cidade] = {'normalizado': nome_normalizado, 'similaridade': score}

    df_completo['city_normalizado'] = df_completo['city'].map(lambda x: mapeamento[x]['normalizado'])
    df_completo['similaridade'] = df_completo['city'].map(lambda x: mapeamento[x]['similaridade'])

    df_baixa_similaridade = df_completo[df_completo['similaridade'] < 50]
    df_completo = df_completo[df_completo['similaridade'] >= 50]

    tipos = {
        'phone': NVARCHAR(15),
        'date': NVARCHAR(10),
        'campaignName': NVARCHAR(100),
        'city': NVARCHAR(50),
        'status': NVARCHAR(20),
        'origin': NVARCHAR(20),
        'id': NVARCHAR(50),
        'ibge': NVARCHAR(7)
    }

    df_completo.to_sql("macro_campanhas_airflow", engine, if_exists='replace', index=False, schema='eaf_tvro', dtype=tipos)

    return df_baixa_similaridade.to_dict(), len(df_completo)


def enviar_mensagem(**kwargs):
    import requests
    import pandas as pd
    from airflow.models import Variable

    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='task_tratamentos_envio_banco')
    df_dict, quantidade_registros = result
    df = pd.DataFrame(df_dict)

    if quantidade_registros > 0:
        chat_id = Variable.get('chat_id')
        token = Variable.get('token_telegram')
        message = f'Foram inseridos {quantidade_registros} registros na tabela macro_campanhas_airflow.'
        url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
        requests.get(url)

    print('quantidade de registros com baixa similaridade:', len(df))
    print(df[['city', 'city_normalizado', 'similaridade']])


default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
}

dag = DAG(
    'macro_campanhas',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)

task_extrair_dados_api = PythonOperator(
    task_id='task_extrair_dados_api',
    python_callable=extrair_dados_api,
    dag=dag
)

task_extrair_dados_sql_server = PythonOperator(
    task_id='task_extrair_dados_sql_server',
    python_callable=extrair_dados_sql_server,
    dag=dag
)

task_tratamentos_envio_banco = PythonVirtualenvOperator(
    task_id='task_tratamentos_envio_banco',
    python_callable=tratamentos_envio_banco,
    dag=dag,
    requirements=['rapidfuzz'],
    op_args=[
        "{{ ti.xcom_pull(task_ids='task_extrair_dados_api') }}",
        "{{ ti.xcom_pull(task_ids='task_extrair_dados_sql_server') }}"
    ],
    templates_dict=None,
)

task_enviar_mensagem = PythonOperator(
    task_id='task_enviar_mensagem',
    python_callable=enviar_mensagem,
    dag=dag
)

task_extrair_dados_api >> task_extrair_dados_sql_server >> task_tratamentos_envio_banco >> task_enviar_mensagem