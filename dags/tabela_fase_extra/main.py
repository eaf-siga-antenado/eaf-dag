import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def atualizar_instalacoes_cpf():
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
        DISTINCT
            f.CPF
    FROM [eaf_tvro].[Freshdesk] f
    JOIN [eaf_tvro].[tabela_fase_extra] tfe
    ON f.CPF = tfe.cpf
    WHERE 1=1
    AND [Status] = 'INSTALLED'
    AND [StatusdaInstalação] = 'Instalada'
    AND [MotivodocontatoInstalação] IN ('Agendamento', 'Interferência')
    AND tfe.status_cpf <> 'Instalado'
    '''
    resultado = session.execute(text(consulta_sql))
    df_instalacoes_cpf = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(df_instalacoes_cpf))

    if len(df_instalacoes_cpf) > 0:
        for _, row in df_instalacoes_cpf.iterrows():
            cpf = row['CPF']
            update_sql = f'''
            UPDATE [eaf_tvro].[tabela_fase_extra]
            SET status_cpf = 'Instalado'
            WHERE cpf = '{cpf}'
            '''
            session.execute(text(update_sql))
        session.commit()

def atualizar_instalacoes_cod_familia():
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
        DISTINCT
	    f.CodFamilia
    FROM [eaf_tvro].[Freshdesk] f
    JOIN [eaf_tvro].[tabela_fase_extra] tfe
    ON TRY_CAST(f.CodFamilia AS bigint) = tfe.cod_familia
    WHERE 1=1
    AND [Status] = 'INSTALLED'
    AND [StatusdaInstalação] = 'Instalada'
    AND [MotivodocontatoInstalação] IN ('Agendamento', 'Interferência')
    AND tfe.status_codfamilia <> 'Instalado'
    '''
    resultado = session.execute(text(consulta_sql))
    df_instalacoes_cod_familia = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(df_instalacoes_cod_familia))

    if len(df_instalacoes_cod_familia) > 0:
        for _, row in df_instalacoes_cod_familia.iterrows():
            cof_familia = row['CodFamilia']
            update_sql = f'''
            UPDATE [eaf_tvro].[tabela_fase_extra]
            SET status_codfamilia = 'Instalado'
            WHERE cod_familia = '{cof_familia}'
            '''
            session.execute(text(update_sql))
        session.commit()
    

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'alertas_power_automate',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False
)

atualizar_instalacoes_cpf = PythonOperator(
    task_id='atualizar_instalacoes_cpf',
    python_callable=atualizar_instalacoes_cpf,
    dag=dag
) 

atualizar_instalacoes_cod_familia = PythonOperator(
    task_id='atualizar_instalacoes_cod_familia',
    python_callable=atualizar_instalacoes_cod_familia,
    dag=dag,
) 

atualizar_instalacoes_cpf >> atualizar_instalacoes_cod_familia
