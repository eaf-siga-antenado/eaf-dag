import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {}
dag = DAG(
    dag_id="campanhas_macro_extrator",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Todo dia às 6h da manhã
    catchup=False,
    start_date=datetime(2025, 10, 23),
)

def main():
    import os
    import logging
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine, VARCHAR, text, NVARCHAR
    from airflow.models import Variable
    import smtplib
    from email.message import EmailMessage

    # Configurações Banco de Dados
    DBSERVER = Variable.get("DBSERVER")
    DATABASE = Variable.get("DATABASE")
    DBUSER = Variable.get("DBUSER")
    DBPASSWORD = Variable.get("DBPASSWORD")
    
    # Configurações Email
    EMAIL_REMETENTE = Variable.get("EMAIL_REMETENTE_RELATORIO")
    SENHA_EMAIL = Variable.get("SENHA_EMAIL_RELATORIO")

    data_execucao = datetime.now()
    
    # Logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("campanhas_macro")

    logger.info("📞 Iniciando Extrator de Campanhas Macro...")
    logger.info(f"⏰ Hora da execução: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
    
    try:
        # Conecta ao SQL Server
        logger.info("Conectando ao SQL Server...")
        engine = create_engine(f'mssql+pyodbc://{DBUSER}:{DBPASSWORD}@{DBSERVER}:1433/{DATABASE}?driver=ODBC Driver 17 for SQL Server')
        
        # Configuração da API
        url_base = "https://api-eaf.azurewebsites.net/tracking/campaigns"
        headers = {
            "accept": "*/*",
            'Authorization': 's&dsdsa@123iudhasdiahsgd#@!'
        }

        # Define período: últimos 30 dias
        data_fim = datetime.now().date()
        data_inicio = data_fim - timedelta(days=30)
        
        params = {
            "startDate": data_inicio.strftime("%Y-%m-%d"),
            "endDate": data_fim.strftime("%Y-%m-%d"),
            "take": 5000
        }

        logger.info(f"🗓️ Extraindo campanhas do período: {data_inicio} até {data_fim}")

        # Extração paginada da API
        skip = 0
        df_final = pd.DataFrame()
        total_requisicoes = 0

        while True:
            params["skip"] = skip
            total_requisicoes += 1
            
            logger.info(f"📡 Requisição {total_requisicoes} - Skip: {skip}")
            response = requests.get(url_base, headers=headers, params=params)

            if response.status_code != 200:
                logger.error(f"❌ Erro {response.status_code}: {response.text}")
                break

            data = response.json()

            if not data or len(data.get('data', [])) == 0:
                logger.info("✅ Fim dos dados - última página atingida")
                break

            df = pd.DataFrame(data)
            df_final = pd.concat([df_final, df], ignore_index=True)
            logger.info(f"📊 Total de registros acumulados: {len(df_final)}")

            skip += params["take"]

        logger.info(f"📞 Total de registros extraídos da API: {len(df_final)}")

        if len(df_final) == 0:
            logger.warning("⚠️ Nenhum registro encontrado na API")
            return

        # Processa dados da API
        df_final = pd.DataFrame(df_final['data'].tolist())
        df_final['date'] = pd.to_datetime(df_final['date']).dt.date
        
        logger.info(f"📋 Dados processados: {len(df_final)} registros")

        # Limpeza de nomes de cidades (correção de caracteres especiais)
        logger.info("🧹 Aplicando correções de nomes de cidades...")
        correcoes_cidades = {
            'An�sio de Abreu': 'Anísio de Abreu',
            'Apicum-A�u': 'Apicum-Açu',
            'Cai�ara do Norte': 'Caiçara do Norte',
            'Centro Novo do Maranh�o': 'Centro Novo do Maranhão',
            'Fartura do Piau�': 'Fartura do Piauí',
            'Flor�nia': 'Florânia',
            'Itaipava do Graja�': 'Itaipava do Grajaú',
            'Jatob�': 'Jatobá',
            'Jo�o Costa': 'João Costa',
            'J�lio Borges': 'Júlio Borges',
            'Maraj� do Sena': 'Marajá do Sena',
            'Palmeira do Piau�': 'Palmeira do Piauí',
            'Morro Cabe�a no Tempo': 'Morro Cabeça no Tempo',
            'Pedro do Ros�rio': 'Pedro do Rosário',
            'Reden��o do Gurgu�ia': 'Redenção do Gurguéia',
            'Ribeiro Gon�alves': 'Ribeiro Gonçalves',
            'Santana do Maranh�o': 'Santana do Maranhão',
            'Santo Amaro do Maranh�o': 'Santo Amaro do Maranhão',
            'Serrano do Maranh�o': 'Serrano do Maranhão',
            'S�o Gon�alo do Gurgu�ia': 'São Gonçalo do Gurguéia',
            'Turia�u': 'Turiaçu',
            'Sao Miguel do Gostoso': 'São Miguel do Gostoso',
            'Quiterianopolis': 'Quiterianópolis',
            'Croata': 'Croatá',
            'Anisio de Abreu': 'Anísio de Abreu',
            'Tamboril do Piau�': 'Tamboril do Piauí',
            'Porto Rico do Maranh�o': 'Porto Rico do Maranhão',
            'Arame ': 'Arame'
        }
        
        for cidade_errada, cidade_correta in correcoes_cidades.items():
            df_final['city'].replace(cidade_errada, cidade_correta, inplace=True)

        # Consulta códigos IBGE
        logger.info("🏘️ Consultando códigos IBGE...")
        Session = sessionmaker(bind=engine)
        session = Session()

        consulta_sql = """
        SELECT
            cIBGE ibge,
            nome_cidade
        FROM [eaf_tvro].[ibge]
        WHERE cIBGE in (
        SELECT
            cIBGE ibge
        FROM [eaf_tvro].[ibge_fase_extra]
        WHERE Fase = 'Etapa A'
        )
        """
        resultado = session.execute(text(consulta_sql))
        ibge = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
        logger.info(f"📍 {len(ibge)} códigos IBGE encontrados")

        # Merge com códigos IBGE
        df_final = df_final.merge(ibge, how='left', left_on='city', right_on='nome_cidade')
        df_final.drop(columns=['nome_cidade'], inplace=True)
        
        # Remove duplicatas
        len_antes = len(df_final)
        df_final.drop_duplicates(inplace=True)
        len_depois = len(df_final)
        logger.info(f"🔄 Duplicatas removidas: {len_antes - len_depois}")

        # Correções específicas de IBGE
        df_final.loc[
            (df_final['city'] == 'Santa Filomena') & (df_final['ibge'].isna()), 
            'ibge'
        ] = '2208908'
        
        # Preenche IBGEs faltantes com código padrão
        df_final['ibge'].fillna('2612554', inplace=True)

        # Define tipos de dados para inserção
        tipos = {
            'phone': NVARCHAR(15),
            'date': NVARCHAR(10),
            'campaignName': NVARCHAR(100),
            'city': NVARCHAR(50),
            'status': NVARCHAR(20),
            'id': NVARCHAR(50),
            'ibge': NVARCHAR(7)
        }

        # Insere no banco de dados
        logger.info("💾 Inserindo dados na tabela macro_campanhas...")
        df_final.to_sql("macro_campanhas", engine, if_exists='replace', index=False, schema='eaf_tvro', dtype=tipos)
        
        # Estatísticas finais
        campanhas_por_nome = df_final['campaignName'].value_counts()
        total_registros = len(df_final)
        
        logger.info(f"✅ Processo concluído!")
        logger.info(f"📊 Total de registros inseridos: {total_registros}")
        logger.info(f"📞 Campanhas encontradas: {len(campanhas_por_nome)}")

        # Função para enviar e-mail de relatório
        def enviar_email_relatorio():
            try:
                msg = EmailMessage()
                msg["Subject"] = f"📞 Extrator Campanhas Macro - Concluído {data_execucao.strftime('%d/%m/%Y %H:%M')}"
                msg["From"] = EMAIL_REMETENTE
                msg["To"] = "marcelo.ferreira.terceirizado@eaf.org.br"
                
                corpo_email = f"""
Prezados,

Segue o relatório do Extrator de Campanhas Macro executado com sucesso.

📊 RESUMO DA EXECUÇÃO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}
- Período extraído: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}
- Total de requisições à API: {total_requisicoes}
- Registros extraídos: {total_registros}
- Tabela atualizada: eaf_tvro.macro_campanhas

📞 TOP 10 CAMPANHAS:
"""
                
                for i, (campanha, quantidade) in enumerate(campanhas_por_nome.head(10).items(), 1):
                    corpo_email += f"{i}. {campanha}: {quantidade:,} registros\n"

                corpo_email += f"""

🔍 DETALHES TÉCNICOS:
- Conexão SQL Server: {DBSERVER}
- Database: {DATABASE}
- Schema: eaf_tvro
- Método: Substituição completa da tabela (REPLACE)

✅ Status: Processo executado com sucesso!
"""

                msg.set_content(corpo_email)

                with smtplib.SMTP("smtp.office365.com", 587) as smtp:
                    smtp.starttls()
                    smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
                    smtp.send_message(msg)

                logger.info("✅ E-mail de relatório enviado com sucesso!")
            except Exception as e:
                logger.error(f"❌ Erro ao enviar e-mail: {e}")

        # Envia o e-mail de sucesso
        enviar_email_relatorio()

        session.close()
        logger.info("📞 Extrator de Campanhas Macro finalizado!")

    except Exception as e:
        logger.error(f"❌ Erro no processo: {e}")
        
        # Envia e-mail de erro
        try:
            msg = EmailMessage()
            msg["Subject"] = f"❌ ERRO - Extrator Campanhas Macro {data_execucao.strftime('%d/%m/%Y %H:%M')}"
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = "marcelo.ferreira.terceirizado@eaf.org.br"
            
            corpo_email = f"""
Prezados,

ERRO no Extrator de Campanhas Macro.

📊 DETALHES DO ERRO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}
- Erro: {str(e)}

❌ Status: Processo executado com falha!

Por favor, verificar os logs do Airflow para mais detalhes.
"""
            msg.set_content(corpo_email)

            with smtplib.SMTP("smtp.office365.com", 587) as smtp:
                smtp.starttls()
                smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
                smtp.send_message(msg)

            logger.info("📧 E-mail de erro enviado!")
        except Exception as email_error:
            logger.error(f"❌ Erro ao enviar e-mail de erro: {email_error}")
        
        raise

# Task do Airflow
campanhas_extrator_task = PythonVirtualenvOperator(
    task_id="campanhas_macro_extrator",
    python_callable=main,
    requirements=["pandas==1.5.3", "requests==2.31.0", "sqlalchemy==1.4.49", "pyodbc==4.0.39"],
    system_site_packages=True,
    dag=dag,
)

campanhas_extrator_task