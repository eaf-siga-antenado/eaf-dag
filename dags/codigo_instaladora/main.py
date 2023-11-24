import pandas as pd
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

def tratamento_dados():
    def extrair_instaladora(valor):
        partes = valor.split(' - ')
        return partes[0].strip().capitalize()
    
    import pandas as pd
    from datetime import date

    df = pd.read_csv('grupos.csv')
    df['instaladora'] = df['name'].apply(extrair_instaladora)
    df.drop(columns='name', inplace=True)
    df['data_atualizacao'] = date.today().strftime("%d-%m-%Y")
    return df

def extrair_dados_api():
    import requests
    import pandas as pd
    from airflow.models import Variable

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
    df_groups.to_csv("grupos.csv", index=False, mode='w', encoding='utf-8', header=True)

def envio_banco_dados(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='tratamento_dados')
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    df.to_sql("grupo_instaladora", engine, if_exists='replace', schema='eaf_tvro', index=False)

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'grupo_instaladoras',
    default_args=default_args,
    schedule_interval='20 9 * * *',
    catchup=False
)

extrair_dados_api = PythonVirtualenvOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_api,
    system_site_packages=True,
    requirements=['requests', 'pandas'],
    dag=dag
)

envio_banco_dados = PythonOperator(
    task_id='envio_banco_dados',
    python_callable=envio_banco_dados,
    dag=dag
)

tratamento_dados = PythonVirtualenvOperator(
    task_id='tratamento_dados',
    python_callable=tratamento_dados,
    system_site_packages=True,
    requirements='pandas',
    dag=dag
) 

extrair_dados_api >> tratamento_dados >> envio_banco_dados
