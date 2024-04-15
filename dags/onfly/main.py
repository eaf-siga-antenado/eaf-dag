import os
import json
import requests
import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator

def token_acesso():
    client_id_onfly = Variable.get("client_id_onfly")
    client_secret_onfly = Variable.get("client_secret_onfly")
    parametro = {
        'grant_type': 'client_credentials',
        'scope': "*",
        'client_id': client_id_onfly,
        'client_secret': client_secret_onfly,
    }

    url = requests.post('https://api.onfly.com.br/oauth/token',data = parametro)
    dados = url.json()
    access_token = dados.get('access_token')
    return access_token

def colaboradores(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    resposta_colaboradores = requests.get('https://api.onfly.com.br/employees?include=document', headers=headers, params=params)
    total_paginas_colaboradores = resposta_colaboradores.json()['meta']['pagination']['total_pages']
    df_colaboradores = pd.DataFrame() 
    dados_pagina = resposta_colaboradores.json()['data']
    for i in range(1, total_paginas_colaboradores+1):
        params['page'] = i
        resposta = requests.get('https://api.onfly.com.br/employees?include=document', headers=headers, params=params)
        dados_pagina = resposta.json()['data']
        df_pagina = pd.DataFrame(dados_pagina)
        df_colaboradores = pd.concat([df_colaboradores, df_pagina], ignore_index=True)
    
    df_colaboradores.drop(columns='document', axis=1, inplace=True)
    return df_colaboradores

def centro_custo(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_centro_custo = pd.DataFrame()
    headers = {'Authorization': access_token}
    url_centrocusto ='https://api.onfly.com.br/settings/cost-center'
    resposta_centrocusto = requests.request('GET',url_centrocusto, headers=headers)
    total_paginas_centrocusto = resposta_centrocusto.json()['meta']['pagination']['total_pages']
    for i in range(1, total_paginas_centrocusto+1):
        params['page'] = i
        url_centrocusto = f'https://api.onfly.com.br/settings/cost-center'
        resposta = requests.request('GET', url_centrocusto, headers=headers, params=params)
        dados_pagina = resposta.json()['data']
        df_pagina = pd.DataFrame(dados_pagina)
        df_centro_custo = pd.concat([df_centro_custo, df_pagina], ignore_index=True)
    
    return df_centro_custo

def grupo(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_grupo = pd.DataFrame()
    resposta_grupo = requests.get('https://api.onfly.com.br/employee-groups', headers=headers)
    total_paginas_grupo = resposta_grupo.json()['meta']['pagination']['total_pages']

    for i in range(1, total_paginas_grupo+1):
        params['page'] = i
        resposta = requests.get('https://api.onfly.com.br/employee-groups', headers=headers, params=params)
        dados_pagina = resposta.json()['data']
        df_pagina = pd.DataFrame(dados_pagina)
        df_grupo = pd.concat([df_grupo, df_pagina], ignore_index=True)
    df_grupo = df_grupo.explode('employeesIds') 
    return df_grupo

def despesa(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_despesa = pd.DataFrame()
    resposta_despesa = requests.get('https://api.onfly.com.br/expense/expenditure', headers=headers, params=params)
    total_paginas_despesa = resposta_despesa.json()['meta']['pagination']['total_pages']

    for i in range(1, total_paginas_despesa+1):
        params['page'] = i
        resposta_despesa = requests.get('https://api.onfly.com.br/expense/expenditure', headers=headers, params=params)
        dados_pagina_despesa = resposta_despesa.json()['data']
        df_pagina_despesa = pd.DataFrame(dados_pagina_despesa)
        df_despesa = pd.concat([df_despesa, df_pagina_despesa], ignore_index=True)
    df_despesa['amount'] = df_despesa['amount'].astype(float) #tranformar coluna amount para float
    df_despesa['amount'] = (df_despesa['amount']/100).map('{:,.2f}'.format) #formatar número coluna amount
    return df_despesa

def viagens_aereo(**kwargs):
    import warnings
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_viagens_aereo = pd.DataFrame()
    resposta_viagens = requests.get('https://api.onfly.com.br/travel/order/fly-order/?include=travellers', headers=headers, params=params)
    total_paginas_viagens = resposta_viagens.json()['meta']['pagination']['total_pages']

    warnings.simplefilter(action='ignore', category=FutureWarning)
    for i in range(1, total_paginas_viagens+1):
        params['page'] = i
        resposta_viagens= requests.get('https://api.onfly.com.br/travel/order/fly-order/?include=travellers', headers=headers, params=params)
        dados_viagens = json.loads(resposta_viagens.text)
        df_pagina = pd.DataFrame(pd.json_normalize(dados_viagens['data']))

        #normalizar as colunas
        df_travellersdata_normalizado = pd.DataFrame(pd.json_normalize(df_pagina['travellers.data']))
        df_travellersdata_normalizado1 = pd.DataFrame(pd.json_normalize(df_travellersdata_normalizado[0],record_prefix='travellers.data'))
        df_viagens_concatenado = pd.concat([df_pagina, df_travellersdata_normalizado1.add_prefix('travellers.')],axis=1)
        df_viagens_concatenado = df_viagens_concatenado.drop(columns=['travellers.data'])
        df_viagens_aereo = pd.concat([df_viagens_aereo, df_viagens_concatenado], axis=0)

        excluir_colunas = ['ticketOutbound','ticketInbound','outbound.services','validationPolicy','inbound.services','outbound.stops']
        df_viagens_aereo = df_viagens_aereo.drop(columns=[coluna for coluna in excluir_colunas if coluna in df_viagens_aereo.columns])
    
    #formatar número coluna
    df_viagens_aereo['travellers.amount'] = (df_viagens_aereo['travellers.amount']/100).map('{:,.2f}'.format) 
    df_viagens_aereo['inbound.priceOnfly'] = (df_viagens_aereo['inbound.priceOnfly']/100).map('{:,.2f}'.format)
    df_viagens_aereo['inbound.priceCia'] = (df_viagens_aereo['inbound.priceCia'] /100).map('{:,.2f}'.format)
    df_viagens_aereo['amount'] = (df_viagens_aereo['amount']/100).map('{:,.2f}'.format)
    df_viagens_aereo['netAmount'] = (df_viagens_aereo['netAmount']/100).map('{:,.2f}'.format)
    warnings.resetwarnings()
    return df_viagens_aereo

def automovel(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_automovel = pd.DataFrame()
    resposta_automovel = requests.get('https://api.onfly.com.br/travel/order/auto-order?include=client', headers=headers, params=params)
    total_paginas_automovel = resposta_automovel.json()['meta']['pagination']['total_pages']

    for i in range(1, total_paginas_automovel+1):
        params['page'] = i
        resposta_automovel = requests.get('https://api.onfly.com.br/travel/order/auto-order?include=client', headers=headers, params=params)
        dados_automovel = json.loads(resposta_automovel.text)
        df_pagina = pd.DataFrame(pd.json_normalize(dados_automovel['data']))
        df_automovel = pd.concat([df_automovel,df_pagina],axis=0,ignore_index=True)

    df_automovel['amount'] = (df_automovel['amount']/100).map('{:,.2f}'.format)
    df_automovel['netAmount'] = (df_automovel['netAmount']/100).map('{:,.2f}'.format)
    df_automovel['amountPerDay'] = (df_automovel['amountPerDay'] /100).map('{:,.2f}'.format)
    df_automovel['dailyAmount'] = (df_automovel['dailyAmount'] /100).map('{:,.2f}'.format)
    return df_automovel

def onibus(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_onibus = pd.DataFrame()
    resposta_onibus = requests.get('https://api.onfly.com.br/travel/order/bus-order?include=client', headers=headers, params=params)
    total_paginas_onibus = resposta_onibus.json()['meta']['pagination']['total_pages']
    
    for i in range(1, total_paginas_onibus+1):
        resposta_onibus = requests.get('https://api.onfly.com.br/travel/order/bus-order?include=client', headers=headers)
        dados_onibus = json.loads(resposta_onibus.text)
        df_pagina = pd.DataFrame(pd.json_normalize(dados_onibus['data']))
        df_onibus = pd.concat([df_onibus,df_pagina],axis=0,ignore_index=True)

    df_onibus['amount'] = (df_onibus['amount']/100).map('{:,.2f}'.format) #formatar número coluna amount
    return df_onibus

def fatura(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    resposta_fatura = requests.get('https://api.onfly.com.br/financial/invoice/list/invoice?iclude=details', headers=headers)
    total_paginas_fatura = resposta_fatura.json()['meta']['pagination']['total_pages']
    dados_pagina_fatura = resposta_fatura.json()['data']
    df_fatura = pd.DataFrame(dados_pagina_fatura)

    for i in range(1, total_paginas_fatura+1):
        params['page'] = i
        resposta_fatura = requests.get('https://api.onfly.com.br/financial/invoice/list/invoice?iclude=details', headers=headers)
        dados_pagina_fatura = resposta_fatura.json()['data']
        df_pagina_fatura = pd.DataFrame(dados_pagina_fatura)
        df_fatura = pd.concat([df_fatura,df_pagina_fatura], ignore_index=True)
    df_fatura['amount'] = (df_fatura['amount']/100).map('{:,.2f}'.format)
    return df_fatura

def creditos(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    df_creditos  = pd.DataFrame()
    resp = requests.get('https://api.onfly.com.br/credits/groupByConsumer', headers=headers, params=params)
    total_paginas = resp.json()['meta']['pagination']['total_pages']

    for i in range(1, total_paginas+1):
        params['page'] = i
        resp = requests.get('https://api.onfly.com.br/credits/groupByConsumer', headers=headers, params=params)
        dict = json.loads(resp.text)
        df = pd.DataFrame(dict['data'])
        df_normalize_credits = pd.json_normalize(df['credits'])
        df = pd.concat([df,df_normalize_credits],axis=1).drop(['credits'], axis=1)
        df_normalize_data = pd.json_normalize(df['data'])
        df = pd.concat([df,df_normalize_data],axis=1).drop(['data'],axis=1)
        df_normalize_0 = pd.json_normalize(df[0])
        df_normalize_0 = df_normalize_0[['id','totalAmount','description','user']]
        df_normalize_0 = df_normalize_0.rename(columns={'id':'id_0','totalAmount':'totalAmount_0','description':'description_0','user':'user_0'})
        pagina = pd.concat([df,df_normalize_0],axis=1)
        pagina = pagina[['id','name','createdAt','updatedAt','id_0','totalAmount_0','description_0','user_0']]
        df_creditos = pd.concat([df_creditos, pagina], ignore_index=True)
    return df_creditos  

def politicas(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    resp = requests.get('https://api.onfly.com.br/settings/travel-policy/approval-group?', headers=headers, params=params)
    dict = json.loads(resp.text)
    df = pd.DataFrame(dict['data'])

    #abrir dicionários que estão como registros nas colunas
    df_normalize_policy = pd.json_normalize(df['policy'])
    df_normalize_costCenterSubjects = pd.json_normalize(df['costCenterSubjects'])
    df_normalize_userSubjects = pd.json_normalize(df['userSubjects'])                           
    df_normalize_levels = pd.json_normalize(df['levels']) 
    df_normalize_confirmationUser = pd.json_normalize(df['confirmationUser'])
    #concatenar dfs que abrimos os dicionarios
    df_politica = pd.concat([df,df_normalize_policy,df_normalize_userSubjects,df_normalize_levels,df_normalize_confirmationUser], axis = 1).drop(['policy','userSubjects','levels','confirmationUser','failOverApprovalGroup','costCenterSubjects'],axis=1)
    #colunas a serem consideradas no df
    colunas_finais = ['id','type','isActive','name','createdAt','updatedAt','policyId','highPrioritySubjectGroupId','lowPrioritySubjectGroupId','confirmationUserId','data.name','data.description','data.isActive',
                        'data.inUse','data.createdAt','data.rules.data.hasToApproveExpenditure']
    df_politica = df_politica[colunas_finais]

    #renomear colunas duplicadas para desconsiderar no df
    df_politica.columns.values[11] = 'data.name_delete'
    df_politica.columns.values[16] = 'data.createdAt_delete'

    #deletar colunas 
    df_politica.drop(columns=['data.name_delete','data.createdAt_delete'],axis=1, inplace=True)
    return df_politica

def envio_banco_dados(**kwargs):

    ti = kwargs['ti']
    colaboradores = ti.xcom_pull(task_ids='colaboradores')
    viagens_aereo = ti.xcom_pull(task_ids='viagens_aereo')
    centro_custo = ti.xcom_pull(task_ids='centro_custo')
    grupo = ti.xcom_pull(task_ids='grupo')
    despesa = ti.xcom_pull(task_ids='despesa')
    automovel = ti.xcom_pull(task_ids='automovel')
    onibus = ti.xcom_pull(task_ids='onibus')
    fatura = ti.xcom_pull(task_ids='fatura')
    creditos = ti.xcom_pull(task_ids='creditos')
    politicas = ti.xcom_pull(task_ids='politicas')

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE-RH')
    username = Variable.get('DBUSER-RH')
    password = Variable.get('DBPASSWORD-RH')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    with engine.connect() as connection:
        
        connection.execute("IF OBJECT_ID('dbo.colaboradores', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.colaboradores END")
        connection.execute("IF OBJECT_ID('dbo.viagens_aereo', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.viagens_aereo END")
        connection.execute("IF OBJECT_ID('dbo.centro_custo', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.centro_custo END")
        connection.execute("IF OBJECT_ID('dbo.grupo', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.grupo END")
        connection.execute("IF OBJECT_ID('dbo.despesa', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.despesa END")
        connection.execute("IF OBJECT_ID('dbo.automovel', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.automovel END")
        connection.execute("IF OBJECT_ID('dbo.onibus', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.onibus END")
        connection.execute("IF OBJECT_ID('dbo.fatura', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.fatura END")
        connection.execute("IF OBJECT_ID('dbo.creditos', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.creditos END")
        connection.execute("IF OBJECT_ID('dbo.politicas', 'U') IS NOT NULL BEGIN DELETE TABLE dbo.politicas END")
    
    colaboradores.to_sql("colaboradores", engine, if_exists='append', index=False)
    viagens_aereo.to_sql("viagens_aereo", engine, if_exists='append', index=False)
    centro_custo.to_sql("centro_custo", engine, if_exists='append', index=False)
    grupo.to_sql("grupo", engine, if_exists='append', index=False)
    despesa.to_sql("despesa", engine, if_exists='append', index=False)
    automovel.to_sql("automovel", engine, if_exists='append', index=False)
    onibus.to_sql("onibus", engine, if_exists='append', index=False)
    fatura.to_sql("fatura", engine, if_exists='append', index=False)
    creditos.to_sql("creditos", engine, if_exists='append', index=False)
    politicas.to_sql("politicas", engine, if_exists='append', index=False)

default_args = {
    'start_date': datetime(2023, 8, 1, 6, 0, 0)
    # 'retries': 2,
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'onfly',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False
)

token_acesso = PythonOperator(
    task_id='token_acesso',
    python_callable=token_acesso,
    dag=dag
)

colaboradores = PythonOperator(
    task_id='colaboradores',
    python_callable=colaboradores,
    dag=dag
)

centro_custo = PythonOperator(
    task_id='centro_custo',
    python_callable=centro_custo,
    dag=dag
)

grupo = PythonOperator(
    task_id='grupo',
    python_callable=grupo,
    dag=dag
)

despesa = PythonOperator(
    task_id='despesa',
    python_callable=despesa,
    dag=dag
)

viagens_aereo = PythonOperator(
    task_id='viagens_aereo',
    python_callable=viagens_aereo,
    dag=dag
)

automovel = PythonOperator(
    task_id='automovel',
    python_callable=automovel,
    dag=dag
)

onibus = PythonOperator(
    task_id='onibus',
    python_callable=onibus,
    dag=dag
)

fatura = PythonOperator(
    task_id='fatura',
    python_callable=fatura,
    dag=dag
)

creditos = PythonOperator(
    task_id='creditos',
    python_callable=creditos,
    dag=dag
)

politicas = PythonOperator(
    task_id='politicas',
    python_callable=politicas,
    dag=dag
)

envio_banco_dados = PythonOperator(
    task_id='envio_banco_dados',
    python_callable=envio_banco_dados,
    dag=dag
)

token_acesso >> colaboradores >> viagens_aereo >> centro_custo >> grupo >> despesa >> automovel >> onibus >> fatura >> creditos >> politicas >> envio_banco_dados