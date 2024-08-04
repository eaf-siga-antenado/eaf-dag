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
            t.B_IBGE IBGE_Fresh,
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
    print('quantidade de tickets duplicados:', len(df_oss_duplicadas))
    return df_oss_duplicadas

def verfica_oss_duplicadas(**kwargs):
    import pyodbc
    import pandas as pd
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

    # preciso verificar se os dois id's que estão na tabela oss_duplicadas, existem no df df_oss_alertadas
    if(len(df_oss_duplicadas)) > 0:
        for _, linha in df_oss_duplicadas.iterrows():
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
    schedule_interval= None, #'*/30 * * * *',
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
