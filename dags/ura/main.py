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
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

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

    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")

    data_anterior = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')

    message = f"EAF-TVRO - URA Datametrica: \n\n Não foi possível atualizar a tabela ura_datametrica, pois não temos informações para o dia {data_anterior}."

    print(message)

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

def mensagem_com_info(**kwargs):

    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")

    ti = kwargs['ti']
    df_api = ti.xcom_pull(task_ids='extrair_dados_api')

    data_anterior = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')

    message = f"EAF-TVRO - URA Datametrica: \n\n Tabela atualizada com sucesso, foram inseridos {len(df_api)} novos registros referente à data {data_anterior}."

    print(message)

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

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
    
    return final

def quantidade_registros(**kwargs):
    ti = kwargs['ti']
    df_api = ti.xcom_pull(task_ids='extrair_dados_api')
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
    'start_date': datetime(2023, 8, 1, 6, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ura_datametrica',
    default_args=default_args,
    schedule_interval='0 16 * * *',
    catchup=False
)

extrair_dados_api = PythonVirtualenvOperator(
    task_id='extrair_dados_api',
    python_callable=extrair_dados_api,
    system_site_packages=True,
    requirements='pandas',
    dag=dag
)

quantidade_registros = BranchPythonOperator(
    task_id='quantidade_registros',
    python_callable=quantidade_registros,
    provide_context=True,
    dag=dag
)

envio_banco_dados = PythonOperator(
    task_id='envio_banco_dados',
    python_callable=envio_banco_dados,
    dag=dag
) 

tratamento_de_dados = PythonOperator(
    task_id='tratamento_de_dados',
    python_callable=tratamento_de_dados,
    dag=dag
) 

mensagem_sem_info = PythonOperator(
    task_id='mensagem_sem_info',
    python_callable=mensagem_sem_info,
    dag=dag
)

mensagem_com_info = PythonOperator(
    task_id='mensagem_com_info',
    python_callable=mensagem_com_info,
    dag=dag
)

mensagem_api_fora_do_ar = PythonOperator(
    task_id='mensagem_api_fora_do_ar',
    python_callable=mensagem_api_fora_do_ar,
    dag=dag
)

status_api = BranchPythonOperator(
    task_id='status_api',
    python_callable=status_api,
    dag=dag
)

verifica_data_banco = BranchPythonOperator(
    task_id='verifica_data_banco',
    python_callable=verifica_data_banco,
    dag=dag
)

nao_faz_nada = DummyOperator(
    task_id='nao_faz_nada',
    dag=dag,
)

verifica_data_banco >> [status_api, nao_faz_nada] 
status_api >> [extrair_dados_api, mensagem_api_fora_do_ar] 
extrair_dados_api >> quantidade_registros >> [mensagem_sem_info, tratamento_de_dados]  
tratamento_de_dados >> envio_banco_dados >> mensagem_com_info
