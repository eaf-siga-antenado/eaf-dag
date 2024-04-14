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
    print(access_token)
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'page':1}
    url_colaboradores = 'https://api.onfly.com.br/employees?include=document'
    resposta_colaboradores = requests.get(url_colaboradores, headers=headers, params=params)
    total_paginas_colaboradores = resposta_colaboradores.json()['meta']['pagination']['total_pages']
    print('total_paginas_colaboradores', total_paginas_colaboradores)
    df_colaboradores = pd.DataFrame() 
    url_colaboradores = 'https://api.onfly.com.br/employees?include=document'
    resposta_colaboradores = requests.request('GET',url_colaboradores, headers=headers, params=params)
    total_paginas_colaboradores = resposta_colaboradores.json()['meta']['pagination']['total_pages']
    dados_pagina = resposta_colaboradores.json()['data']
    for i in range(1, total_paginas_colaboradores+1):
        headers = {'Authorization': access_token}
        params['page'] = i
        url_colaboradores = f'https://api.onfly.com.br/employees?include=document'
        resposta = requests.request('GET', url_colaboradores, headers=headers, params=params)
        dados_pagina = resposta.json()['data']
        df_pagina = pd.DataFrame(dados_pagina)
        df_colaboradores = pd.concat([df_colaboradores, df_pagina], ignore_index=True)

    return colaboradores

def centro_custo(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='token_acesso')
    headers = {'Authorization': access_token}
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
    print('QUANTIDADE DE INFORMAÇÕES NO CENTRO DE CUSTO:')
    print(len(df_centro_custo))
    df_centro_custo.head()


def verifica_data_banco():
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = 'SELECT MAX(CAST(dataHoraInicio AS DATE)) data FROM eaf_tvro.ura_datametrica'
    resultado = session.execute(text(consulta_sql))
    data_maxima = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())

    data_hoje = datetime.today().date()

    diferenca = (data_hoje - data_maxima.data[0]).days
    print(f'a diferença entre dias é de: {diferenca}')

    if diferenca >= 2:
        return 'status_api'
    return 'nao_faz_nada'

def mensagem_api_fora_do_ar():

    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")

    message = f"EAF-TVRO - URA Datametrica: \n\n Não foi possível atualizar a tabela ura_datametrica, pois a API está for do ar."

    print(message)

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

def mensagem_sem_info():
    pass

def mensagem_com_info(**kwargs):
    pass

def parse_datetime(value):
    try:
        if pd.notna(value):
            cleaned_value = value.replace('T', ' ').split('.')[0]
            return pd.to_datetime(cleaned_value)
        else:
            return pd.NaT
    except:
        return pd.NaT
    
def status_api():
    login = json.loads(Variable.get("api_datametrica"))

    try:
        url_autenticacao = 'https://api-eaf-extrator-genesys.datametrica.com.br/login'
        response = requests.post(url_autenticacao, json=login)
        mensagem = 'ok'
    except Exception as e:
        mensagem = e
    if str(mensagem).startswith("HTTPSConnectionPool"):
        return 'mensagem_api_fora_do_ar'
    return 'extrair_dados_api'

def extrair_dados_api():

    import os
    import json
    import requests
    import pandas as pd
    from airflow.models import Variable
    from datetime import datetime, timedelta
    from concurrent.futures import ThreadPoolExecutor
    
    login = json.loads(Variable.get("api_datametrica"))

    url_autenticacao = 'https://api-eaf-extrator-genesys.datametrica.com.br/login'
    response = requests.post(url_autenticacao, json=login)
    jwt_token = response.json()

    inicio_ontem = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    final_ontem = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    formato = '%Y-%m-%d %H:%M:%S'
    inicio_ontem = inicio_ontem.strftime(formato)
    final_ontem = final_ontem.strftime(formato)
    
    params = {
        "dataHoraInicio": inicio_ontem,
        "dataHoraFim": final_ontem,
        "pagina": 1
    }

    headers = {'Authorization': f'Bearer {jwt_token["access_token"]}'}
    url_recursos_protegidos = 'https://api-eaf-extrator-genesys.datametrica.com.br/chamadas'
    response_recursos = requests.post(url_recursos_protegidos, headers=headers, json=params)
    num_paginas = response_recursos.json()['totalPaginas']

    final = pd.DataFrame()

    def fetch_data(page):
        params['pagina'] = page
        print(params)
        response_recursos = requests.post(url_recursos_protegidos, headers=headers, json=params)
        df = pd.DataFrame(response_recursos.json()['content'])
        return df

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        dfs = executor.map(fetch_data, range(1, num_paginas+1))

    final = pd.concat(list(dfs), ignore_index=True)
    print('QUANTIDADE DE INFORMAÇÕES:', len(final))
    
    return final

def quantidade_registros(**kwargs):
    ti = kwargs['ti']
    df_api = ti.xcom_pull(task_ids='extrair_dados_api')
    print('QUANTIDADE DE INFORMAÇÕES:', len(df_api))
    if len(df_api) == 0:
        return 'mensagem_sem_info'
    return 'tratamento_de_dados'

def tratamento_de_dados(**kwargs):

    def parse_datetime(value):
        try:
            if pd.notna(value):
                cleaned_value = value.replace('T', ' ')
                return pd.to_datetime(cleaned_value)
            else:
                return pd.NaT
        except:
            return pd.NaT
        
    ti = kwargs['ti']
    final = ti.xcom_pull(task_ids='extrair_dados_api')
    print('os dados chegaram aqui:', len(final))
    print(final.head())

    # convertendo colunas de datas
    final['dataHoraInicio'] = final['dataHoraInicio'].apply(parse_datetime)
    final['dataHoraFim'] = final['dataHoraFim'].apply(parse_datetime)
    final['data_atualizacao'] = date.today().strftime("%d-%m-%Y")

    # tratamento para a coluna atendimento
    for index, row in final.iterrows():
        try:
            tipo = []
            tempoEspera = []
            tempoAtendimento = []
            atendidaAte60Seg = []
            desistenciaAte60Seg = []
            atendidaPos60Seg = []
            desistenciaPos60Seg = []

            for _, i in pd.DataFrame(row['atendimento']).iterrows():
                tipo.append(i['tipo'])
                tempoEspera.append(i['tempoEspera'])
                tempoAtendimento.append(i['tempoAtendimento'])
                atendidaAte60Seg.append(i['atendidaAte60Seg'])
                desistenciaAte60Seg.append(i['desistenciaAte60Seg'])
                atendidaPos60Seg.append(i['atendidaPos60Seg'])
                desistenciaPos60Seg.append(i['desistenciaPos60Seg'])

            # Atribuir os valores para a linha específica do DataFrame
            final.at[index, 'tipo'] = str(tipo)
            final.at[index, 'tempoEspera'] = str(tempoEspera)
            final.at[index, 'tempoAtendimento'] = str(tempoAtendimento)
            final.at[index, 'atendidaAte60Seg'] = str(atendidaAte60Seg)
            final.at[index, 'desistenciaAte60Seg'] = str(desistenciaAte60Seg)
            final.at[index, 'atendidaPos60Seg'] = str(atendidaPos60Seg)
            final.at[index, 'desistenciaPos60Seg'] = str(desistenciaPos60Seg)

        except:
            # Se ocorrer uma exceção, atribuir None para as colunas
            final.at[index, 'tipo'] = None
            final.at[index, 'tempoEspera'] = None
            final.at[index, 'tempoAtendimento'] = None
            final.at[index, 'atendidaAte60Seg'] = None
            final.at[index, 'desistenciaAte60Seg'] = None
            final.at[index, 'atendidaPos60Seg'] = None
            final.at[index, 'desistenciaPos60Seg'] = None

    # tratamento para a coluna interlocutor
    for index, row in final.iterrows():
        try:
            origem = []
            cadastroLocalizadoEAF = []
            uf = []
            ddd = []
            codigoCidadeIBGE = []
            cpf = []
            cep = []
            telefone = []

            for _, i in pd.DataFrame([row['interlocutor']]).iterrows():
                origem.append(i['origem'])
                cadastroLocalizadoEAF.append(i['cadastroLocalizadoEAF'])
                uf.append(i['uf'])
                ddd.append(i['ddd'])
                codigoCidadeIBGE.append(i['codigoCidadeIBGE'])
                cpf.append(i['codigoCidadeIBGE'])
                cep.append(i['cep'])
                telefone.append(i['telefone'])

            # Atribuir os valores para a linha específica do DataFrame
            final.at[index, 'origem'] = str(origem)
            final.at[index, 'cadastroLocalizadoEAF'] = str(cadastroLocalizadoEAF)
            final.at[index, 'uf'] = str(uf)
            final.at[index, 'ddd'] = str(ddd)
            final.at[index, 'codigoCidadeIBGE'] = str(codigoCidadeIBGE)
            final.at[index, 'cpf'] = str(cpf)
            final.at[index, 'cep'] = str(cep)
            final.at[index, 'telefone'] = str(telefone)

        except:
            # Se ocorrer uma exceção, atribuir None para as colunas
            final.at[index, 'origem'] = None
            final.at[index, 'cadastroLocalizadoEAF'] = None
            final.at[index, 'uf'] = None
            final.at[index, 'ddd'] = None
            final.at[index, 'codigoCidadeIBGE'] = None
            final.at[index, 'cpf'] = None
            final.at[index, 'cep'] = None
            final.at[index, 'telefone'] = None
    final.drop(columns=['atendimento', 'interlocutor', 'csv'], inplace=True)

    def remove_colchetes(valor):
        if valor == None:
            return valor
        return valor.replace('[', '').replace(']', '').replace("'", "")

    # Aplicar a função a todos os elementos do DataFrame
    final['tipo'] = final['tipo'].apply(remove_colchetes)
    final['tempoEspera'] = final['tempoEspera'].apply(remove_colchetes)
    final['tempoAtendimento'] = final['tempoAtendimento'].apply(remove_colchetes)
    final['atendidaAte60Seg'] = final['atendidaAte60Seg'].apply(remove_colchetes)
    final['desistenciaAte60Seg'] = final['desistenciaAte60Seg'].apply(remove_colchetes)
    final['atendidaPos60Seg'] = final['atendidaPos60Seg'].apply(remove_colchetes)
    final['desistenciaPos60Seg'] = final['desistenciaPos60Seg'].apply(remove_colchetes)
    final['origem'] = final['origem'].apply(remove_colchetes)
    final['cadastroLocalizadoEAF'] = final['cadastroLocalizadoEAF'].apply(remove_colchetes)
    final['uf'] = final['uf'].apply(remove_colchetes)
    final['ddd'] = final['ddd'].apply(remove_colchetes)
    final['codigoCidadeIBGE'] = final['codigoCidadeIBGE'].apply(remove_colchetes)
    final['cpf'] = final['cpf'].apply(remove_colchetes)
    final['cep'] = final['cep'].apply(remove_colchetes)
    final['telefone'] = final['telefone'].apply(remove_colchetes)
    final.rename({'tempoEspera': 'tempoEspera_ms', 'tempoAtendimento': 'tempoAtendimento_ms'}, axis=1, inplace=True)
    return final

def envio_banco_dados(**kwargs):

    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids='tratamento_de_dados')
    # output.drop(columns=['id', 'hash', 'csv', 'midia'], axis=1, inplace=True)
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    # output['data_atualizacao'] = date.today().strftime("%d-%m-%Y")
    output.to_sql("ura_datametrica", engine, if_exists='append', schema='eaf_tvro', index=False)

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

token_acesso >> colaboradores >> centro_custo
