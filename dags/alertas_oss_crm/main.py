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
            t.[IDdoticket] OsFresh,
            t.StatusdaInstalação StatusInstalacaoFresh,
            CAST(t.Horadetérminodocompromisso AS DATE) DataInstalacaoFresh,
            g.instaladora InstaladoraFresh,
            t.B_NOME NomeFresh,
            t.B_CPF CPF_Fresh,
            n.[IDdoticket] OsCRM_EAF,
            n.Status StatusCRM_EAF,
            n.StatusDaInstalacao StatusInstalacaoCRM_EAF,
            n.HoraDaCriacao CriacaoCRM_EAF,
            n.DataHoraAgendamento DataAgendamentoCRM_EAF,
            n.B_Nome NomeCRM_EAF,
            n.B_IBGE IBGE_CRM_EAF,
            n.Instaladora InstaladoraCRM_EAF,
            t.B_CPF as CPF_CRM_EAF,
            i.fase AS FaseCidade
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
    schedule_interval= None, #'*/30 * * * *',
    catchup=False
)

oss_duplicadas = PythonOperator(
    task_id='oss_duplicadas',
    python_callable=oss_duplicadas,
    dag=dag
)

oss_duplicadas 