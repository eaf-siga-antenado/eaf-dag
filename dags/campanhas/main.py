import requests
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, NVARCHAR
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator

def extrair_dados_api():
    import requests
    import pandas as pd
    from datetime import date
    from sqlalchemy.orm import sessionmaker
    from airflow.models import Variable
    from rapidfuzz import process, fuzz
    from sqlalchemy import create_engine, text, NVARCHAR
    
    url_base = "https://api-eaf.azurewebsites.net/tracking/campaigns"
    headers = {
        "accept": "*/*",
        'Authorization': 's&dsdsa@123iudhasdiahsgd#@!'
    }

    hoje = date.today().strftime("%Y-%m-%d")

    params = {
        "startDate": "2025-08-01",
        "endDate": f"{hoje}",
        "take": 5000
    }
    print('parametros:', params)
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
        print(f"Registros coletados até agora: {len(df_final)}")
        skip += params["take"]
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
    df_final = pd.DataFrame(df_final['data'].tolist())
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
    cidades_unicas = df_final['city'].unique()
    mapeamento = {}
    for cidade in cidades_unicas:
        nome_normalizado, score = normalizar_cidade(cidade)
        mapeamento[cidade] = {'normalizado': nome_normalizado, 'similaridade': score}
    df_final['city_normalizado'] = df_final['city'].map(lambda x: mapeamento[x]['normalizado'])
    df_final['similaridade'] = df_final['city'].map(lambda x: mapeamento[x]['similaridade'])
    df_baixa_similaridade = df_final[df_final['similaridade'] < 50]
    df_final = df_final[df_final['similaridade'] >= 50]
    df_final = df_final.merge(ibge, how='left', left_on='city_normalizado', right_on='nome_cidade')
    df_final.drop(columns=['nome_cidade', 'similaridade', 'city'], inplace=True)
    df_final.rename(columns={'city_normalizado': 'city'}, inplace=True)
    df_final.drop_duplicates(inplace=True)
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
    print('df completo')
    print(df_final.head())
    df_final['date'] = pd.to_datetime(df_final['date'], format='mixed').dt.date
    df_final.to_sql("macro_campanhas_airflow", engine, if_exists='replace', index=False, schema='eaf_tvro', dtype=tipos)
    return df_baixa_similaridade, len(df_final)
 
def enviar_mensagem(**kwargs):
    import requests
    from airflow.models import Variable
    ti = kwargs['ti']
    df, quantidade_registros = ti.xcom_pull(task_ids='extrair_dados_api')
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
    schedule_interval='0 10 * * *',
    catchup=False
)
extrair_dados_api = PythonVirtualenvOperator(
    task_id='extrair_dados_api',
    python_callable=extrair_dados_api,
    requirements = ['rapidfuzz'],
    dag=dag
)

enviar_mensagem = PythonOperator(
    task_id='enviar_mensagem',
    python_callable=enviar_mensagem,
    dag=dag
)

extrair_dados_api >> enviar_mensagem
