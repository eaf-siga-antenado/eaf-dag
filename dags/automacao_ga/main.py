import pandas as pd
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

def extrair_ibge_banco():

    import pandas as pd
    from unidecode import unidecode
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    # puxar dados do banco
    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = 'SELECT LOWER(Nome_Cidade) nome_cidade, MAX(cIBGE) cIBGE FROM [eaf_tvro].[ibge] GROUP BY LOWER(Nome_Cidade)'
    resultado = session.execute(text(consulta_sql))
    ibge = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    ibge.rename(columns={'cIBGE': 'ibge', 'Nome_Cidade': 'nome_cidade', 'UF': 'uf'}, inplace=True)
    session.close()

    ibge['nome_cidade'] = ibge['nome_cidade'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    return ibge

def extrair_dados_ga():

    import json
    import numpy as np
    import pandas as pd
    from datetime import datetime
    from datetime import timedelta
    from airflow.models import Variable
    from google.oauth2 import service_account
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import Metric
    from google.analytics.data_v1beta.types import OrderBy
    from google.analytics.data_v1beta.types import DateRange
    from google.analytics.data_v1beta.types import Dimension
    from google.analytics.data_v1beta.types import RunReportRequest

    property_id = Variable.get("property_id_nova")
    chave = Variable.get("chave", deserialize_json=True)
    chave = json.loads(json.dumps(chave))
    chave = service_account.Credentials.from_service_account_info(chave)

    dia_anterior = datetime.now() - timedelta(days=1)
    dia_anterior = dia_anterior.strftime("%Y-%m-%d")

    request = RunReportRequest(

    property='properties/' + property_id,
    dimensions=[Dimension(name="date"), Dimension(name="customEvent:event_label"), Dimension(name="region"),  Dimension(name="city"),],
    metrics=[Metric(name="eventCount"),],
    order_bys=[OrderBy(dimension={'dimension_name': 'date'}),],
    date_ranges=[DateRange(start_date=dia_anterior, end_date=dia_anterior)],)

    client = BetaAnalyticsDataClient(credentials=chave)

    response = client.run_report(request)

    # Row index
    row_index_names = [header.name for header in response.dimension_headers]
    row_header = []

    for i in range(len(row_index_names)):
        row_header.append([row.dimension_values[i].value for row in response.rows])

    row_index_named = pd.MultiIndex.from_arrays(np.array(row_header), names=np.array(row_index_names))
    metric_names = [header.name for header in response.metric_headers]
    data_values = []

    for i in range(len(metric_names)):
        data_values.append([row.metric_values[i].value for row in response.rows])
    output = pd.DataFrame(data=np.transpose(np.array(data_values, dtype='f')), index=row_index_named, columns=metric_names)

    output.reset_index(inplace=True)
    output['date'] = pd.to_datetime(output['date']).dt.date
    output.rename({'customEvent:event_label': 'eventName'}, axis=1, inplace=True)
    output['city'] = output['city'].str.lower()
    
    return output

def envio_banco_dados(**kwargs):

    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids='extrair_dados')
    ibge = ti.xcom_pull(task_ids='extrair_ibge_banco')
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')

    subir = output.merge(ibge, left_on='city', right_on='nome_cidade', how='left')
    subir['ibge'] = subir['ibge'].astype(pd.Int64Dtype())
    subir['eventCount'] = subir['eventCount'].astype(pd.Int64Dtype())
    subir.drop(columns='city', inplace=True, axis=1)

    subir.to_sql("google_analytics_events", engine, if_exists='append', schema='eaf_tvro', index=False)

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'google_analytics_events',
    default_args=default_args,
    schedule_interval='20 9 * * *',
    catchup=False
)

dados_ga = PythonVirtualenvOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados_ga,
    system_site_packages=True,
    requirements='requirements.txt',
    dag=dag
)

envio_banco_dados = PythonOperator(
    task_id='envio_banco_dados',
    python_callable=envio_banco_dados,
    dag=dag
)

extrair_ibge_banco = PythonVirtualenvOperator(
    task_id='extrair_ibge_banco',
    python_callable=extrair_ibge_banco,
    system_site_packages=True,
    requirements='requirements.txt',
    dag=dag
) 

dados_ga >> extrair_ibge_banco >> envio_banco_dados
