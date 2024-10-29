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
            t.[IDdoticket] AS OsFresh,
            t.StatusdaInstalação AS StatusInstalacaoFresh,
            t.B_IBGE AS IBGE_Fresh,
            CAST(t.Horadetérminodocompromisso AS DATE) AS DataInstalacaoFresh,
            g.instaladora AS InstaladoraFresh,
            t.B_NOME AS NomeFresh,
            t.B_CPF AS CPF_Fresh,
            n.service_order_number AS OsCRM_EAF,
            n.service_order_status AS StatusCRM_EAF,
            n.installer_response_classification AS StatusInstalacaoCRM_EAF,
            CAST(n.service_order_created_at AS DATE) AS CriacaoCRM_EAF,
            CAST(n.schedule_date_time AS DATE) AS DataAgendamentoCRM_EAF,
            n.customer_name AS NomeCRM_EAF,
            NULLIF(
                CASE 
                    WHEN n.customer_ibge IS NULL OR TRIM(n.customer_ibge) = '' THEN NULLIF(pci.[IBGE], '') 
                    ELSE n.customer_ibge 
                END, ''
            ) AS IBGE_CRM_EAF,
            n.installer AS InstaladoraCRM_EAF,
            t.B_CPF AS CPF_CRM_EAF,
            i.fase AS FaseCidade
        FROM [eaf_tvro].[ticket] AS t
        JOIN [eaf_tvro].[crm_ticket_data] AS n ON t.[B_CPF] = n.customer_cpf
        LEFT JOIN [eaf_tvro].[grupo_instaladora] AS g ON t.[Grupo] = g.[id]
        LEFT JOIN [eaf_tvro].[ibge] AS i ON t.[B_IBGE] = i.[cIBGE]
        LEFT JOIN [eaf_tvro].[postal_code_ibge] AS pci ON TRIM(REPLACE(n.[customer_postal_code], '-', '')) = TRIM(REPLACE(pci.[postal_code], '-', ''))
        WHERE 
            t.[Status] IN ('4', '5')
            AND t.[MotivodocontatoInstalação] NOT IN (
                'Cobrança Indevida',
                'Contestação',
                'Manutenção',
                'Manutenção - Problema Técnico',
                'Manutenção - Técnico Não Compareceu',
                'Reclamação por problema técnico'
            )
            AND t.[StatusdaInstalação] = 'Instalada'
            AND n.service_order_number LIKE '2025%'
            AND n.service_order_status NOT LIKE 'CANCELLED%'
			AND n.service_order_type IN ('Agendamento de Instalação', 'Agendamento')
			AND t.IDdoticket <> n.service_order_number
    '''
    resultado = session.execute(text(consulta_sql))
    df_oss_duplicadas = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    print('quantidade de tickets duplicados:', len(df_oss_duplicadas))
    return df_oss_duplicadas

def verfica_oss_duplicadas(**kwargs):
    import pyodbc
    import pandas as pd
    from random import randint
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text

    ti = kwargs['ti']
    df_oss_duplicadas = ti.xcom_pull(task_ids='oss_duplicadas')
    
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')

    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    conn = pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
    cursor = conn.cursor()
    cursor.execute('DELETE [eaf_tvro].[oss_alertas]')
    cursor.commit()
    Session = sessionmaker(bind=engine)
    session = Session()

    consulta_sql = 'SELECT * FROM [eaf_tvro].[oss_duplicadas]'
    resultado = session.execute(text(consulta_sql))
    df_oss_alertadas = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
    quantidade = 0
    valor_maximo = randint(6, 11)

    # preciso verificar se os dois id's que estão na tabela oss_duplicadas, existem no df df_oss_alertadas
    if(len(df_oss_duplicadas)) > 0:
        for _, linha in df_oss_duplicadas.iterrows():
            if quantidade >= valor_maximo:
                break
            ticket_fresh = linha['OsFresh']
            ticket_eaf = linha['OsCRM_EAF']
            if((df_oss_alertadas['OsFresh'] == ticket_fresh) & (df_oss_alertadas['OsCRM_EAF'] == ticket_eaf)).any():
                pass
            else:
                insere_informacao = f"""
                    INSERT INTO eaf_tvro.oss_alertas 
                    ([OsFresh], [StatusInstalacaoFresh], [IBGE_Fresh], [DataInstalacaoFresh], [InstaladoraFresh], [NomeFresh], [CPF_Fresh], 
                    [OsCRM_EAF], [StatusCRM_EAF], [StatusInstalacaoCRM_EAF], [CriacaoCRM_EAF], [DataAgendamentoCRM_EAF], 
                    [NomeCRM_EAF], [IBGE_CRM_EAF], [InstaladoraCRM_EAF], [CPF_CRM_EAF], [FaseCidade])
                    VALUES 
                    ('{linha['OsFresh']}', '{linha['StatusInstalacaoFresh']}', '{linha['IBGE_Fresh']}', '{linha['DataInstalacaoFresh']}', '{linha['InstaladoraFresh']}', 
                    '{linha['NomeFresh']}', '{linha['CPF_Fresh']}', '{linha['OsCRM_EAF']}', '{linha['StatusCRM_EAF']}', 
                    '{linha['StatusInstalacaoCRM_EAF']}', '{linha['CriacaoCRM_EAF']}', '{linha['DataAgendamentoCRM_EAF']}', 
                    '{linha['NomeCRM_EAF']}', '{linha['IBGE_CRM_EAF']}', '{linha['InstaladoraCRM_EAF']}', '{linha['CPF_CRM_EAF']}', 
                    '{linha['FaseCidade']}');
                    """
                cursor.execute(insere_informacao)
                cursor.commit()

                insere_informacao = f"""
                    INSERT INTO eaf_tvro.oss_duplicadas 
                    ([OsFresh], [StatusInstalacaoFresh], [IBGE_Fresh], [DataInstalacaoFresh], [InstaladoraFresh], [NomeFresh], [CPF_Fresh], 
                    [OsCRM_EAF], [StatusCRM_EAF], [StatusInstalacaoCRM_EAF], [CriacaoCRM_EAF], [DataAgendamentoCRM_EAF], 
                    [NomeCRM_EAF], [IBGE_CRM_EAF], [InstaladoraCRM_EAF], [CPF_CRM_EAF], [FaseCidade])
                    VALUES 
                    ('{linha['OsFresh']}', '{linha['StatusInstalacaoFresh']}', '{linha['IBGE_Fresh']}', '{linha['DataInstalacaoFresh']}', '{linha['InstaladoraFresh']}', 
                    '{linha['NomeFresh']}', '{linha['CPF_Fresh']}', '{linha['OsCRM_EAF']}', '{linha['StatusCRM_EAF']}', 
                    '{linha['StatusInstalacaoCRM_EAF']}', '{linha['CriacaoCRM_EAF']}', '{linha['DataAgendamentoCRM_EAF']}', 
                    '{linha['NomeCRM_EAF']}', '{linha['IBGE_CRM_EAF']}', '{linha['InstaladoraCRM_EAF']}', '{linha['CPF_CRM_EAF']}', 
                    '{linha['FaseCidade']}');
                    """
                cursor.execute(insere_informacao)
                cursor.commit()
                quantidade += 1
    else:
        for _, linha in df_oss_duplicadas.iterrows():
            insere_informacao = f"""
                INSERT INTO eaf_tvro.oss_alertas 
                ([OsFresh], [StatusInstalacaoFresh], [IBGE_Fresh], [DataInstalacaoFresh], [InstaladoraFresh], [NomeFresh], [CPF_Fresh], 
                [OsCRM_EAF], [StatusCRM_EAF], [StatusInstalacaoCRM_EAF], [CriacaoCRM_EAF], [DataAgendamentoCRM_EAF], 
                [NomeCRM_EAF], [IBGE_CRM_EAF], [InstaladoraCRM_EAF], [CPF_CRM_EAF], [FaseCidade])
                VALUES 
                ('{linha['OsFresh']}', '{linha['StatusInstalacaoFresh']}', '{linha['IBGE_Fresh']}', '{linha['DataInstalacaoFresh']}', '{linha['InstaladoraFresh']}', 
                '{linha['NomeFresh']}', '{linha['CPF_Fresh']}', '{linha['OsCRM_EAF']}', '{linha['StatusCRM_EAF']}', 
                '{linha['StatusInstalacaoCRM_EAF']}', '{linha['CriacaoCRM_EAF']}', '{linha['DataAgendamentoCRM_EAF']}', 
                '{linha['NomeCRM_EAF']}', '{linha['IBGE_CRM_EAF']}', '{linha['InstaladoraCRM_EAF']}', '{linha['CPF_CRM_EAF']}', 
                '{linha['FaseCidade']}');
                """
            cursor.execute(insere_informacao)
            cursor.commit()

            insere_informacao = f"""
                INSERT INTO eaf_tvro.oss_duplicadas 
                ([OsFresh], [StatusInstalacaoFresh], [IBGE_Fresh], [DataInstalacaoFresh], [InstaladoraFresh], [NomeFresh], [CPF_Fresh], 
                [OsCRM_EAF], [StatusCRM_EAF], [StatusInstalacaoCRM_EAF], [CriacaoCRM_EAF], [DataAgendamentoCRM_EAF], 
                [NomeCRM_EAF], [IBGE_CRM_EAF], [InstaladoraCRM_EAF], [CPF_CRM_EAF], [FaseCidade])
                VALUES 
                ('{linha['OsFresh']}', '{linha['StatusInstalacaoFresh']}', '{linha['IBGE_Fresh']}', '{linha['DataInstalacaoFresh']}', '{linha['InstaladoraFresh']}', 
                '{linha['NomeFresh']}', '{linha['CPF_Fresh']}', '{linha['OsCRM_EAF']}', '{linha['StatusCRM_EAF']}', 
                '{linha['StatusInstalacaoCRM_EAF']}', '{linha['CriacaoCRM_EAF']}', '{linha['DataAgendamentoCRM_EAF']}', 
                '{linha['NomeCRM_EAF']}', '{linha['IBGE_CRM_EAF']}', '{linha['InstaladoraCRM_EAF']}', '{linha['CPF_CRM_EAF']}', 
                '{linha['FaseCidade']}');
                """
            cursor.execute(insere_informacao)
            cursor.commit()
            quantidade += 1
    return quantidade

def envia_info_power_automate(**kwargs):
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, text
    ti = kwargs['ti']
    quantidade = ti.xcom_pull(task_ids='verfica_oss_duplicadas')
    print('VALOR DE QUANTIDADE EH:', quantidade)
    server = Variable.get('DBSERVER')
    database = Variable.get('DATABASE')
    username = Variable.get('DBUSER')
    password = Variable.get('DBPASSWORD')
    engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC Driver 18 for SQL Server')
    if quantidade > 0:
        df_power_automate = pd.DataFrame({'dataHora_disparo': [(datetime.now() - timedelta(hours=3))], 'qtd_alertas': [quantidade]})
        df_power_automate.to_sql("power_automate_crm", engine, if_exists='append', schema='eaf_tvro', index=False)
    

default_args = {
    'start_date': datetime(2023, 8, 18, 6, 0, 0),
    'retries': None
}

dag = DAG(
    'alertas_oss_email',
    default_args=default_args,
    schedule_interval= '0 * * * *',
    catchup=False
)

oss_duplicadas = PythonOperator(
    task_id='oss_duplicadas',
    python_callable=oss_duplicadas,
    dag=dag
)

verfica_oss_duplicadas = PythonOperator(
    task_id='verfica_oss_duplicadas',
    python_callable=verfica_oss_duplicadas,
    dag=dag
)

envia_info_power_automate = PythonOperator(
    task_id='envia_info_power_automate',
    python_callable=envia_info_power_automate,
    dag=dag
)

oss_duplicadas >> verfica_oss_duplicadas >> envia_info_power_automate
