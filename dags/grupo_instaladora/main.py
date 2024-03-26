import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

def extrair_dados_api():
    import requests
    import pandas as pd
    from datetime import date
    from airflow.models import Variable
    from sqlalchemy import create_engine, text

    url = "https://sigaantenado.freshdesk.com/api/v2/groups/"
    content_type = Variable.get("CONTENT_TYPE")
    authorization_basic = Variable.get("AUTHORIZATION_BASIC")
    headers = {
        "Content-Type": content_type,
        "Authorization": authorization_basic
    }
    
    groups = []
    page = 1
    per_page = 100

    while True:
        params = {
            "page": page,
            "per_page": per_page
        }
        response = requests.get(url, headers=headers, params=params)
        groups_json = response.json()
    
        if not groups_json:
            break
    
        for group in groups_json:
            groups.append({'id': group['id'], 'name': group['name']})
    
        page += 1
    
    df_groups = pd.DataFrame(groups)

    def extrair_instaladora(valor):
        partes = valor.split(' - ')
        return partes[0].strip().capitalize()

    df_groups['instaladora'] = df_groups['name'].apply(extrair_instaladora)
    df_groups.drop(columns='name', inplace=True)
    df_groups['data_atualizacao'] = date.today().strftime("%d-%m-%Y")
    df_groups['id'] = df_groups['id'].astype(str)
    
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    df_groups.to_sql("grupo_instaladora", engine, if_exists='replace', schema='eaf_tvro', index=False)

default_args = {
    'start_date': datetime(2023, 8, 18, 5, 0, 0)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'grupo_instaladoras',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False
)

extrair_dados_api = PythonVirtualenvOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_api,
    system_site_packages=True,
    requirements=['requests', 'pandas'],
    dag=dag
)

extrair_dados_api 
