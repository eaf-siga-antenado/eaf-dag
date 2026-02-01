import json
import requests
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, NVARCHAR
from airflow.operators.python_operator import PythonOperator


def extrair_dados_api():
    """Extrai dados da API de campanhas"""
    url_base = "https://api-eaf.azurewebsites.net/tracking/campaigns"
    
    # Usando Variable para credenciais (mais seguro)
    api_token = Variable.get('API_EAF_TOKEN', default_var='s&dsdsa@123iudhasdiahsgd#@!')
    
    headers = {
        "accept": "*/*",
        'Authorization': api_token
    }
    params = {
        "startDate": "2026-01-25",
        "endDate": "2026-01-31",
        "take": 5000
    }
    
    skip = 0
    todos_registros = []
    
    while True:
        params["skip"] = skip
        response = requests.get(url_base, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Erro na API: {response.status_code}")
            break
            
        data = response.json()
        
        if not data or 'data' not in data or len(data['data']) == 0:
            break
        
        # Adiciona os registros da página atual
        todos_registros.extend(data['data'])
        skip += params["take"]
        
        print(f"Extraídos {len(todos_registros)} registros até agora...")
    
    print(f'Total de registros extraídos da API: {len(todos_registros)}')
    
    # Retorna como lista de dicionários (serializável para XCom)
    return todos_registros


def extrair_dados_sql_server():
    """Extrai dados de IBGE do SQL Server"""
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server'
    engine = create_engine(connection_string)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    consulta_sql = """
        SELECT
            cIBGE AS ibge,
            nome_cidade
        FROM [eaf_tvro].[ibge]
        WHERE cIBGE IN (
            SELECT cIBGE
            FROM [eaf_tvro].[ibge_fase_extra]
        )
    """
    
    resultado = session.execute(text(consulta_sql))
    ibge = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    
    session.close()
    engine.dispose()
    
    print(f'Quantidade de registros extraídos do SQL Server: {len(ibge)}')
    print(ibge.head())
    
    # Retorna como lista de dicionários (serializável para XCom)
    return ibge.to_dict('records')


def tratamentos_envio_banco(**kwargs):
    """Trata os dados e envia para o banco"""
    from rapidfuzz import process, fuzz
    
    print('Iniciando tratamentos_envio_banco')
    
    # Recupera credenciais do banco
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server'
    engine = create_engine(connection_string)
    
    # Recupera dados do XCom
    ti = kwargs['ti']
    
    dados_api = ti.xcom_pull(task_ids='task_extrair_dados_api')
    dados_ibge = ti.xcom_pull(task_ids='task_extrair_dados_sql_server')
    
    # Converte para DataFrames
    df_completo = pd.DataFrame(dados_api)
    ibge = pd.DataFrame(dados_ibge)
    
    print(f'Registros da API: {len(df_completo)}')
    print(df_completo.head())
    
    print(f'Registros IBGE: {len(ibge)}')
    print(ibge.head())
    
    # Lista de cidades oficiais para matching
    cidades_oficiais = ibge['nome_cidade'].tolist()
    
    def normalizar_cidade(cidade):
        """Normaliza o nome da cidade usando fuzzy matching"""
        if pd.isna(cidade):
            return None, 0
        
        resultado = process.extractOne(
            cidade,
            cidades_oficiais,
            scorer=fuzz.token_sort_ratio
        )
        
        if resultado:
            return resultado[0], resultado[1]
        return cidade, 0
    
    # Cria mapeamento para evitar recalcular para cidades duplicadas
    cidades_unicas = df_completo['city'].unique()
    mapeamento = {}
    
    for cidade in cidades_unicas:
        nome_normalizado, score = normalizar_cidade(cidade)
        mapeamento[cidade] = {'normalizado': nome_normalizado, 'similaridade': score}
    
    # Aplica o mapeamento
    df_completo['city_normalizado'] = df_completo['city'].map(
        lambda x: mapeamento.get(x, {}).get('normalizado') if pd.notna(x) else None
    )
    df_completo['similaridade'] = df_completo['city'].map(
        lambda x: mapeamento.get(x, {}).get('similaridade', 0) if pd.notna(x) else 0
    )
    
    # Merge com IBGE para obter o código
    df_completo = df_completo.merge(
        ibge,
        left_on='city_normalizado',
        right_on='nome_cidade',
        how='left'
    )
    
    # Separa registros com baixa similaridade
    df_baixa_similaridade = df_completo[df_completo['similaridade'] < 50].copy()
    df_completo = df_completo[df_completo['similaridade'] >= 50].copy()
    
    print(f'Registros com similaridade >= 50: {len(df_completo)}')
    print(f'Registros com similaridade < 50: {len(df_baixa_similaridade)}')
    
    # Define tipos para o SQL Server
    tipos = {
        'phone': NVARCHAR(15),
        'date': NVARCHAR(10),
        'campaignName': NVARCHAR(100),
        'city': NVARCHAR(50),
        'city_normalizado': NVARCHAR(50),
        'status': NVARCHAR(20),
        'origin': NVARCHAR(20),
        'id': NVARCHAR(50),
        'ibge': NVARCHAR(7)
    }
    
    # Seleciona apenas as colunas necessárias (ajuste conforme sua necessidade)
    colunas_finais = [col for col in tipos.keys() if col in df_completo.columns]
    df_para_salvar = df_completo[colunas_finais].copy()
    
    print('Dados a serem salvos:')
    print(df_para_salvar.head())
    
    # Salva no banco
    df_para_salvar.to_sql(
        "macro_campanhas_airflow",
        engine,
        if_exists='replace',
        index=False,
        schema='eaf_tvro',
        dtype={k: v for k, v in tipos.items() if k in colunas_finais}
    )
    
    engine.dispose()
    
    print(f'Dados salvos com sucesso! Total: {len(df_para_salvar)} registros')
    
    # Retorna informações para a próxima task
    resultado = {
        'quantidade_inseridos': len(df_para_salvar),
        'quantidade_baixa_similaridade': len(df_baixa_similaridade),
        'baixa_similaridade': df_baixa_similaridade[['city', 'city_normalizado', 'similaridade']].head(50).to_dict('records')
    }
    
    return resultado


def enviar_mensagem(**kwargs):
    """Envia mensagem de notificação via Telegram"""
    ti = kwargs['ti']
    resultado = ti.xcom_pull(task_ids='task_tratamentos_envio_banco')
    
    quantidade_registros = resultado.get('quantidade_inseridos', 0)
    quantidade_baixa_similaridade = resultado.get('quantidade_baixa_similaridade', 0)
    
    if quantidade_registros > 0:
        chat_id = Variable.get('chat_id')
        token = Variable.get('token_telegram')
        
        message = (
            f'✅ DAG macro_campanhas executada com sucesso!\n\n'
            f'📊 Registros inseridos: {quantidade_registros}\n'
            f'⚠️ Registros com baixa similaridade: {quantidade_baixa_similaridade}'
        )
        
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        params = {
            'chat_id': chat_id,
            'text': message
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            print('Mensagem enviada com sucesso!')
        else:
            print(f'Erro ao enviar mensagem: {response.status_code}')
    
    # Log dos registros com baixa similaridade
    print(f'\nRegistros com baixa similaridade ({quantidade_baixa_similaridade}):')
    for item in resultado.get('baixa_similaridade', []):
        print(f"  - {item['city']} -> {item['city_normalizado']} (score: {item['similaridade']})")


# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Definição da DAG
dag = DAG(
    'macro_campanhas',
    default_args=default_args,
    description='Extrai dados de campanhas da API e processa com matching de cidades',
    schedule_interval='0 11 * * *',
    catchup=False,
    tags=['campanhas', 'api', 'ibge']
)

# Definição das tasks (nomes diferentes das funções para evitar conflito)
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

task_tratamentos_envio_banco = PythonOperator(
    task_id='task_tratamentos_envio_banco',
    python_callable=tratamentos_envio_banco,
    provide_context=True,
    dag=dag
)

task_enviar_mensagem = PythonOperator(
    task_id='task_enviar_mensagem',
    python_callable=enviar_mensagem,
    provide_context=True,
    dag=dag
)

# Definição das dependências
task_extrair_dados_api >> task_extrair_dados_sql_server >> task_tratamentos_envio_banco >> task_enviar_mensagem
