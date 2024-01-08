import csv
import requests
import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def extrair_ibge_banco():

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

    consulta_sql = 'SELECT CAST(cIBGE AS int) cIBGE, fase FROM eaf_tvro.ibge'
    resultado = session.execute(text(consulta_sql))
    ibge = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    ibge.rename(columns={'cIBGE': 'ibge'}, inplace=True)
    session.close()

    return ibge

def extrair_dados_api():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios/"
    payload = ""
    headers = {
    "cookie": "_x_w=44_1; _helpkit_session=BAh7B0kiD3Nlc3Npb25faWQGOgZFVEkiJTM2ODNlMGZhYTU2M2MwMTA2ZTQ5YzQ0ZDEzZmMxNzZkBjsAVEkiDnJldHVybl90bwY7AEZJIhsvYXBpL3YyL3RpY2tldHMvMjE0OTMzBjsAVA%253D%253D--c7bf5ca7bb64c92d80051a494967c4c3b61f7434",
    "Content-Type": "application/json",
    "Authorization": "Basic YVQ1S1h5NHQzZjh4MjZlYmFL"
    }
    response = requests.request("GET", url, data=payload, headers=headers)
    tabela_ibge = response.json()

    with open('temp.csv', mode='w', newline='', encoding='utf-8') as file:
        
        writer = csv.writer(file)

        header = ["UF", "Município", "COD_IBGE", "População", "Total Instalações Previstas", "Fase",
                "Fornecedor 1", "Instalações grupo 1", "ID_do_Grupo_1",
                "Fornecedor 2", "Instalações grupo 2", "ID_do_Grupo_2",
                "Fornecedor 3", "Instalações grupo 3", "ID_do_Grupo_3",
                "Fornecedor 4", "Instalações grupo 4", "ID_do_Grupo_4",
                "Fornecedor 5", "Instalações grupo 5", "ID_do_Grupo_5",
                "Fornecedor 6", "Instalações grupo 6", "ID_do_Grupo_6",
                "Fornecedor 7", "Instalações grupo 7", "ID_do_Grupo_7",
                "Fornecedor 8", "Instalações grupo 8", "ID_do_Grupo_8",
                "Fornecedor 9", "Instalações grupo 9", "ID_do_Grupo_9",
                "Fornecedor 10", "Instalações grupo 10", "ID_do_Grupo_10",
                "IBGE Code on List",]
        writer.writerow(header)

        url = "https://sigaantenado.freshdesk.com/api/v2/custom_objects/schemas/163989/records?page_size=100"

        while url:
            try:
                response = requests.get(url, headers=headers)
                data = response.json()

                for record in data['records']:
                    row = [record['data']['uf'], record['data']['nome_da_cidade'], record['data']['ibge'], record['data']['populao'],
                        record['data']['total_de_instalaes_previstas'], record['data']['fases'],
                        record['data']['fornecedor_1'], record['data']['instalaes_dirias_1'], record['data']['id_do_grupo_fornecedor_1'],
                        record['data']['fornecedor_2'], record['data']['instalaes_dirias_fornecedor_2'], record['data']['id_do_grupo_fornecedor_2'],
                        record['data']['fornecedor_3'], record['data']['instalaes_dirrias_fornecedor_3'], record['data']['id_do_grupo_fornecedor_3'],
                        record['data']['fornecedor_4'], record['data']['instalaes_dirias_fornecedor_4'], record['data']['id_do_grupo_fornecedor_4'],
                        record['data']['fornecedor_5'], record['data']['instalaes_dirias_fornecedor_5'], record['data']['id_do_grupo_fornecedor_5'],
                        record['data']['fornecedor_6'], record['data']['instalaes_dirias_fornecedor_6'], record['data']['id_do_grupo_fornecedor_6'],
                        record['data']['fornecedor_7'], record['data']['instalaes_dirias_fornecedor_7'], record['data']['id_do_grupo_fornecedor_7'],
                        record['data']['fornecedor_8'], record['data']['instalaes_dirias_fornecedor_8'], record['data']['id_do_grupo_fornecedor_8'],
                        record['data']['fornecedor_9'], record['data']['instalaes_dirias_fornecedor_9'], record['data']['id_do_grupo_fornecedor_9'],
                        record['data']['fornecedor_10'], record['data']['instalaes_dirias_fornecedor_10'], record['data']['id_do_grupo_fornecedor_10']]

                    found = False
                    for item in tabela_ibge:
                        if record['data']['nome_da_cidade'].split(" - ")[0].lower() == item['nome'].lower() and record['data']['uf'] == item['microrregiao']['mesorregiao']['UF']['sigla']:
                            row.append(item['id'])
                            found = True
                            break
                    if not found:
                        row.append('Não encontrado')

                    writer.writerow(row)

                next_link = data['_links'].get('next', {}).get('href')
                def substring_after(s, delim):
                    return s.partition(delim)[2]
                link = substring_after(next_link, "163989/")

                if next_link:
                    url = "https://sigaantenado.freshdesk.com/api/v2/custom_objects/schemas/163989/" + link
                else:
                    url = None

            except Exception as e:
                url = None 

def tratando_dados(**kwargs):
    ti = kwargs['ti']
    base_ibge = ti.xcom_pull(task_ids='extrair_ibge_banco')
    base = pd.read_csv('temp.csv')
    base.dropna(axis=1, how='all', inplace=True)
    base.drop(columns=['IBGE Code on List', 'Fase'], axis=1, inplace=True)
    final_data = []
    maior_numero = max(int(col.split()[-1]) for col in base.columns if col.startswith('Fornecedor '))

    for _, row in base.iterrows():
        uf = row['UF']
        ibge = row['COD_IBGE']
        municipio = row['Município']
        instalacoes_prev = row['Total Instalações Previstas']
        for col in range(1, maior_numero + 1):
            fornecedor_col = f'Fornecedor {col}'
            instalacoes_col = f'Instalações grupo {col}'
            id_grupo_col = f'ID_do_Grupo_{col}'
            fornecedor = row[fornecedor_col]
            instalacoes = row[instalacoes_col]
            id_grupo = row[id_grupo_col]
            if fornecedor is not None and not pd.isna(instalacoes):
                final_data.append([uf, ibge, municipio, instalacoes_prev, fornecedor, instalacoes, id_grupo])

    df_final = pd.DataFrame(final_data, columns=['uf', 'ibge', 'municipio', 'total_instalacoes_previstas', 'fornecedor', 'instalacoes_dia', 'id_grupo'])
    df_final['instalacoes_dia'] = df_final['instalacoes_dia'].astype(int)
    df_final['id_grupo'] = df_final['id_grupo'].astype('int64')
    df_final['id_grupo'] = df_final['id_grupo'].astype(str)
    df_final = df_final.merge(base_ibge, how='left', on='ibge')
    # df_final = df_final[df_final['instalacoes_dia'] > 0]
    df_final['InstaladoraIBGE'] = df_final['fornecedor'].str.upper() + df_final['ibge'].astype(str)
    df_final['data_atualizacao'] = date.today().strftime("%d-%m-%Y")

    # remover registro, pois o id_grupo 150000383508 está duplicado
    condicao = (df_final['ibge'] == 5201702) & (df_final['municipio'] == 'Aragarças') & (df_final['id_grupo'] == '150000383508')
    df_final = df_final[~condicao]

    return df_final

def envio_banco_dados(**kwargs):

    ti = kwargs['ti']
    subir = ti.xcom_pull(task_ids='tratando_dados')

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    subir.to_sql("capacidade_instaladoras", engine, if_exists='replace', schema='eaf_tvro', index=False)

def mensagem_telegram():
    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")
    data_hoje = datetime.now().strftime('%d-%m-%Y')
    message = f"EAF-TVRO - Capacidade Instaladoras: Tabela atualizada com sucesso! Informações referentes ao dia {data_hoje}."
    print(message)
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'capacidade_instaladoras',
    default_args=default_args,
    schedule_interval='20 11 * * *',
    catchup=False
)

envio_banco_dados = PythonOperator(
    task_id='envio_banco_dados',
    python_callable=envio_banco_dados,
    dag=dag
)

extrair_dados_api = PythonOperator(
    task_id='extrair_dados_api',
    python_callable=extrair_dados_api,
    dag=dag
) 

tratando_dados = PythonOperator(
    task_id='tratando_dados',
    python_callable=tratando_dados,
    dag=dag
) 

mensagem_telegram = PythonOperator(
    task_id='mensagem_telegram',
    python_callable=mensagem_telegram,
    dag=dag
) 

extrair_ibge_banco = PythonOperator(
    task_id='extrair_ibge_banco',
    python_callable=extrair_ibge_banco,
    dag=dag
) 

extrair_dados_api >> extrair_ibge_banco >> tratando_dados >> envio_banco_dados >> mensagem_telegram
