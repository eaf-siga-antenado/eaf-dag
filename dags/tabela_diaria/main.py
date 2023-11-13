import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator

def mensagem_sucesso():

    import requests
    import datetime as dt
    from airflow.models import Variable

    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")

    data_atual = dt.datetime.now()
    data_formatada = data_atual.strftime('%d-%m-%Y')
    message = f"EAF-TVRO - Tabela dos gráficos: \n\n Sucesso ao criar a tabela no dia {data_formatada}"

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

def mensagem_falha():

    import requests
    import datetime as dt
    from airflow.models import Variable

    TOKEN = Variable.get("TELEGRAM_DAILY_STATUS_TOKEN")
    chat_id = Variable.get("TELEGRAM_DAILY_STATUS_ID")

    data_atual = dt.datetime.now()
    data_formatada = data_atual.strftime('%d-%m-%Y')
    message = f"EAF-TVRO - Tabela dos gráficos: \n\n Falha ao criar a tabela no dia {data_formatada}"

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

def cria_tabela_e_notificacao():

    import pyodbc
    from airflow.models import Variable

    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    db_connection_string = (f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TIMEOUT=3600")
    query_deleta = "IF OBJECT_ID('eaf_tvro.InstalacoesPorDia', 'U') IS NOT NULL BEGIN DROP TABLE eaf_tvro.InstalacoesPorDia END"
    query_cria_tabela = "SELECT COUNT(*) as Instalacoes, Horadetérminodocompromisso as DataInstalacao INTO eaf_tvro.InstalacoesPorDia FROM eaf_tvro.TicketsServiceTaskDatas where Status IN ('4', '5') AND StatusdaInstalação = 'Instalada' AND Horadetérminodocompromisso >= '2023-01-30' AND Horadetérminodocompromisso <= CONVERT(varchar(10), GETDATE(), 120) GROUP BY Horadetérminodocompromisso"
   
    conn = pyodbc.connect(db_connection_string)
    cursor = conn.cursor()
    cursor.execute(query_deleta)
    conn.commit()
    cursor.execute(query_cria_tabela)
    conn.commit()
        
default_args = {
    'start_date': dt.datetime(2023, 11, 14, 6, 0, 0),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=3)
}

dag = DAG(
    'tabela_InstalacoesPorDia',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=False
)

cria_tabela = PythonVirtualenvOperator(
    task_id='cria_tabela',
    python_callable=cria_tabela_e_notificacao,
    system_site_packages=True,
    requirements='requirements.txt',
    dag=dag
)

mensagem_sucesso = PythonVirtualenvOperator(
    task_id='mensagem_sucesso',
    python_callable=mensagem_sucesso,
    system_site_packages=True,
    requirements='requirements.txt',
    dag=dag
)

mensagem_falha = PythonVirtualenvOperator(
    task_id='mensagem_falha',
    python_callable=mensagem_falha,
    system_site_packages=True,
    requirements='requirements.txt',
    dag=dag,
    trigger_rule = 'one_failed'
)

cria_tabela >> [mensagem_sucesso, mensagem_falha]
