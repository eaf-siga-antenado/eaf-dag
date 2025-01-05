from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator

def extrair_dados_api():
    import requests
    import pandas as pd
    from io import StringIO
    from datetime import date
    from airflow.models import Variable
    from sqlalchemy import create_engine, text

    menos_30dias = (date.today() + timedelta(days=-30)).strftime('%Y-%m-%d')
    ontem = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

    uri_ativo = f"/metrics/active-identity/D?startDate={menos_30dias}T00%3A00%3A00.000Z&endDate={ontem}T00%3A00%3A00.000Z"
    uri_engajado = f"/metrics/engaged-identity/D?startDate={menos_30dias}T00%3A00%3A00.000Z&endDate={ontem}T00%3A00%3A00.000Z"
    
    url = 'https://macro.http.msging.net/commands'
    headers = {
        "Authorization": "Key c2lnYWFudGVuYWRvcHJkOjVhN280WVc4dDhka3BxaEZZY243",
        "Content-Type": "application/json",
        "Cookie": "__cf_bm=cFRIOVqESGCZMrpeF6XkQp9XccirCBAyMqsLvXUSQko-1731072015-1.0.1.1-MPFHdKinBOM4rJuhSCo4hkRdWbgF66LqdLtP_KS4cndjFp0Nq.FKvgPqqgPDkdoImSoA3PKy6yyfeb6uT5_PHw"
    }
    data = {
        "id": "db56678e-7a11-48fd-bcd8-af9b0c989530",
        "to": "postmaster@analytics.msging.net",
        "method": "get",
        "uri": uri_engajado
    }
    response = requests.post(url, headers=headers, json=data)
    response_data = response.json()

    # Extraindo apenas as informações necessárias
    items = response_data['resource']['items']
    data = [{'intervalStart': item['intervalStart'], 'engajados': item['count']} for item in items]

    # Criando o DataFrame
    df_engajado = pd.DataFrame(data)
    df_engajado['intervalStart'] = pd.to_datetime(df_engajado['intervalStart']).dt.date
    print(len(df_engajado))
    df_engajado.head()

    data = {
    "id": "db56678e-7a11-48fd-bcd8-af9b0c989530",
    "to": "postmaster@analytics.msging.net",
    "method": "get",
    "uri": uri_ativo
    }
    response = requests.post(url, headers=headers, json=data)
    response_data = response.json()

    # Extraindo apenas as informações necessárias
    items = response_data['resource']['items']
    data = [{'intervalStart': item['intervalStart'], 'ativos': item['count']} for item in items]

    # Criando o DataFrame
    df_ativo = pd.DataFrame(data)
    df_ativo['intervalStart'] = pd.to_datetime(df_ativo['intervalStart']).dt.date
    print(len(df_ativo))
    df_ativo.head()

    df_final = df_ativo.merge(df_engajado, on='intervalStart', how='left')
    print(len(df_final))
    df_final.head()

    df_final.rename({'intervalStart': 'data'}, axis=1, inplace=True)
    print(len(df_final))
    df_final.head()

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    
    query = text(f"DELETE FROM [eaf_tvro].[macro_whatsapp] WHERE data >= '{menos_30dias}'")
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()


    df_final.to_sql("macro_whatsapp", engine, if_exists='append', schema='eaf_tvro', index=False)
    

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0)
}

dag = DAG(
    'macro_whatsapp',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)

extrair_dados_api = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_api,
    dag=dag
)

extrair_dados_api 
