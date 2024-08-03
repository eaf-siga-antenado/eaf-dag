import pandas as pd
from airflow import DAG
from datetime import date
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator

def oss_duplicadas():
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
        t.[IDdoticket] ,
        t.Status,
        t.StatusdaInstalação,
        t.Horadacriação,
        t.DataHoraAgendamento,
        t.B_Nome,
        t.B_IBGE AS IBGE,
        g.instaladora AS Instaladora,
        n.[IDdoticket] AS [Ordem de Serviço],
        n.Status AS [Status no Novo CRM],
        n.StatusDaInstalacao as [Status da Instalação no Novo CRM],
        n.HoraDaCriacao [Criação Novo CRM],
        n.DataHoraAgendamento AS [DataAgendamento Novo CRM],
        n.B_Nome as Nome,
        n.B_IBGE [IBGE Novo CRM],
        n.Instaladora,
        t.B_CPF as CPF,
        i.fase AS Fase
    FROM [eaf_tvro].[ticket] t
    JOIN [eaf_tvro].[ticket_novo_crm] n ON t.[B_CPF] = n.[B_CPF]
    LEFT JOIN [eaf_tvro].[grupo_instaladora] g ON t.[Grupo] = g.[id]
    LEFT JOIN [eaf_tvro].[ibge] i ON t.[B_IBGE] = i.[cIBGE]
    WHERE t.[Status] IN ('4', '5')
    AND t.[MotivodocontatoInstalação] NOT IN (
        'Cobrança Indevida',
        'Contestação',
        'Manutenção',
        'Manutenção - Problema Técnico',
        'Manutenção - Técnico Não Compareceu',
        'Reclamação por problema técnico'
    )
    AND t.[StatusdaInstalação] IN ('Instalada')
    '''
    resultado = session.execute(text(consulta_sql))
    df_oss_duplicadas = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print(len(df_oss_duplicadas))
    print(df_oss_duplicadas.head(10))
    return df_oss_duplicadas

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'alertas_oss_email',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False
)

oss_duplicadas = PythonOperator(
    task_id='oss_duplicadas',
    python_callable=oss_duplicadas,
    dag=dag
)

oss_duplicadas 