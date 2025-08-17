import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def criados():
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
    consulta_sql = '''
    SELECT   
	Instaladora,
    ibge,
	UF,
	Municipio,
	Horadacriação,
	Fase,
	'Criados' AS Regra,
	COUNT(*) quantidade
    FROM [eaf_tvro].[FaseExtra]
    WHERE Horadacriação = CAST(GETDATE() -1 AS DATE)
    GROUP BY
    Instaladora,
    ibge,
    UF,
    Municipio,
    Horadacriação,
    Fase
    '''
    resultado = session.execute(text(consulta_sql))
    df_criados = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return df_criados

def aberto():
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

    consulta_sql = '''
    SELECT   
	Instaladora,
    ibge,
	UF,
	Municipio,
	Horadacriação,
	Fase,
	'Em aberto' AS Regra,
	COUNT(*) quantidade
    FROM [eaf_tvro].[FaseExtra]
    WHERE 1=1
    AND Status = 'IN_PROGRESS'
    GROUP BY
    Instaladora,
    ibge,
    UF,
    Municipio,
    Horadacriação,
    Fase
    '''
    resultado = session.execute(text(consulta_sql))
    df_aberto = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return df_aberto

def backlog():
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

    consulta_sql = '''
    SELECT   
	Instaladora,
    ibge,
	UF,
	Municipio,
	Horadacriação,
	Fase,
	'Backlog' AS Regra,
	COUNT(*) quantidade
    FROM [eaf_tvro].[FaseExtra]
    WHERE 1=1
    AND Status = 'IN_PROGRESS'
    AND DataHoraAgendamento < CAST(GETDATE() AS DATE)
    GROUP BY
    Instaladora,
    ibge,
    UF,
    Municipio,
    Horadacriação,
    Fase
    '''
    resultado = session.execute(text(consulta_sql))
    df_backlog = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return df_backlog

def instalados():
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

    consulta_sql = '''
    SELECT   
        Instaladora,
        ibge,
        UF,
        Municipio,
        Horadacriação,
        Fase,
        'Instalados' AS Regra,
        COUNT(*) quantidade
    FROM [eaf_tvro].[FaseExtra]
    WHERE 1=1 
    AND Status = 'INSTALLED'
    AND StatusdaInstalação = 'Instalada'
    AND [MotivodocontatoInstalação] IN ('Agendamento', 'Interferência')
    GROUP BY
    Instaladora,
    ibge,
    UF,
    Municipio,
    Horadacriação,
    Fase
    '''
    resultado = session.execute(text(consulta_sql))
    df_instalados = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    return df_instalados

def historico(**kwargs):
    
    import pandas as pd
    from datetime import datetime
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

    ti = kwargs['ti']
    df_criados = ti.xcom_pull(task_ids='criados')
    df_aberto = ti.xcom_pull(task_ids='aberto')
    df_backlog = ti.xcom_pull(task_ids='backlog')
    df_instalados = ti.xcom_pull(task_ids='instalados')
    df_historico = pd.concat([df_criados, df_aberto, df_backlog, df_instalados], ignore_index=True)
    df_historico['data_registro'] = datetime.now().strftime('%d-%m-%Y')

    df_historico.rename(columns={
        'Instaladora': 'instaladora',
        'UF': 'uf',
        'Municipio': 'municipio',
        'Horadacriação': 'data_criacao',
        'Fase': 'fase',
        'Regra': 'regra',
    }, inplace=True)
    df_historico.to_sql("historico_indicadores", engine, if_exists='append', schema='eaf_tvro', index=False)

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'historico_indicadores',
    default_args=default_args,
    schedule_interval='0 23 * * *',
    catchup=False
)

criados = PythonOperator(
    task_id='criados',
    python_callable=criados,
    dag=dag
) 

aberto = PythonOperator(
    task_id='aberto',
    python_callable=aberto,
    dag=dag
) 

backlog = PythonOperator(
    task_id='backlog',
    python_callable=backlog,
    dag=dag
) 

instalados = PythonOperator(
    task_id='instalados',
    python_callable=instalados,
    dag=dag
) 

historico = PythonOperator(
    task_id='historico',
    python_callable=historico,
    dag=dag
) 

[criados, aberto, backlog, instalados] >> historico
