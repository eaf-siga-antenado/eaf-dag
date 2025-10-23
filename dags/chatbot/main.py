import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable

default_args = {}
dag = DAG(
    dag_id="chatbot_macro_extrator",
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
    logger = logging.getLogger("chatbot_macro")

    logger.info("🤖 Iniciando Extrator de Chatbot Macro...")
    logger.info(f"⏰ Hora da execução: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
    
    try:
        # Conecta ao SQL Server
        logger.info("Conectando ao SQL Server...")
        engine = create_engine(f'mssql+pyodbc://{DBUSER}:{DBPASSWORD}@{DBSERVER}:1433/{DATABASE}?driver=ODBC Driver 17 for SQL Server')
        
        # Configuração da API
        url_base = "https://api-eaf.azurewebsites.net/tracking/consolidatedEvents"
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

        logger.info(f"🗓️ Extraindo eventos consolidados do período: {data_inicio} até {data_fim}")

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

        logger.info(f"🤖 Total de registros extraídos da API: {len(df_final)}")

        if len(df_final) == 0:
            logger.warning("⚠️ Nenhum registro encontrado na API")
            return

        # Processa dados da API (fiel ao notebook original)
        logger.info("🔄 Processando dados da API...")
        df_teste = pd.DataFrame(df_final['data'].tolist())
        logger.info(f"📋 Dados extraídos: {len(df_teste)} registros")

        # Expande dados do CRM (fiel ao notebook)
        logger.info("📊 Expandindo dados do CRM...")
        crm_expandidos = pd.json_normalize(
            [x if isinstance(x, dict) else {} for x in df_teste['crm']]
        )
        df_teste = pd.concat([df_teste.drop(columns=['crm']), crm_expandidos], axis=1)
        logger.info(f"📋 Dados após expansão CRM: {len(df_teste)} registros")

        # Remove colunas desnecessárias (fiel ao notebook)
        logger.info("🧹 Removendo colunas desnecessárias...")
        colunas_remover = ['trackingBlipEafId', 'identity', 'updateDate', 'sessionID', 
                          'errors', 'tracking', 'id', 'uuid', 'ibgeCode', 'familyCode', 'cpf']
        
        # Remove apenas as colunas que existem
        colunas_existentes = [col for col in colunas_remover if col in df_teste.columns]
        if colunas_existentes:
            df_teste.drop(columns=colunas_existentes, inplace=True)
            logger.info(f"🗑️ Removidas {len(colunas_existentes)} colunas: {colunas_existentes}")

        logger.info(f"📋 Dados após limpeza: {len(df_teste)} registros")

        # Remove duplicatas (fiel ao notebook)
        len_antes = len(df_teste)
        df_teste.drop_duplicates(inplace=True)
        len_depois = len(df_teste)
        logger.info(f"🔄 Duplicatas removidas: {len_antes - len_depois}")

        # Formatação de data (fiel ao notebook)
        logger.info("📅 Formatando datas...")
        df_teste['date'] = pd.to_datetime(df_teste['date'], errors='coerce').dt.strftime("%Y-%m-%d %H:%M")
        
        # Remove duplicatas novamente após formatação de data
        len_antes = len(df_teste)
        df_teste.drop_duplicates(inplace=True)
        len_depois = len(df_teste)
        if len_antes != len_depois:
            logger.info(f"🔄 Duplicatas adicionais removidas após formatação: {len_antes - len_depois}")

        # Define tipos de dados (fiel ao notebook)
        tipos = {
            'phone': NVARCHAR(15),
            'date': NVARCHAR(100),
            'channel': NVARCHAR(10),
            'category': NVARCHAR(80),
            'action': NVARCHAR(40),
            'status': NVARCHAR(30),
            'protocol': NVARCHAR(20)
        }

        # Gera arquivo Excel para anexar ao email
        logger.info("📊 Gerando arquivo Excel...")
        excel_filename = f"chatbot_macro_{data_execucao.strftime('%Y-%m-%d_%H-%M')}.xlsx"
        df_teste.to_excel(excel_filename, index=False, sheet_name='Chatbot')

        # Insere no banco de dados (fiel ao notebook original)
        logger.info("💾 Inserindo dados na tabela macro_chatbot...")
        logger.info(f"📊 Inserindo {len(df_teste):,} registros...")
        
        # Insere em chunks para melhor performance
        chunk_size = 10000
        df_teste.to_sql("macro_chatbot", engine, if_exists='replace', index=False, 
                       schema='eaf_tvro', dtype=tipos, chunksize=chunk_size, method='multi')
        
        # Estatísticas finais
        total_registros = len(df_teste)
        categorias_por_nome = df_teste['category'].value_counts() if 'category' in df_teste.columns else pd.Series()
        
        logger.info(f"✅ Processo concluído!")
        logger.info(f"📊 Total de registros inseridos: {total_registros:,}")
        logger.info(f"🤖 Categorias encontradas: {len(categorias_por_nome)}")
        logger.info(f"📁 Arquivo Excel gerado: {excel_filename}")

        # Função para enviar e-mail de relatório
        def enviar_email_relatorio():
            try:
                msg = EmailMessage()
                msg["Subject"] = f"🤖 Extrator Chatbot Macro - Concluído {data_execucao.strftime('%d/%m/%Y %H:%M')}"
                msg["From"] = EMAIL_REMETENTE
                msg["To"] = "marcelo.ferreira.terceirizado@eaf.org.br"
                
                corpo_email = f"""
Prezados,

Segue o relatório do Extrator de Chatbot Macro executado com sucesso.

📊 RESUMO DA EXECUÇÃO:
- Data/Hora: {data_execucao.strftime('%d/%m/%Y %H:%M:%S')}
- Período extraído: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}
- Total de requisições à API: {total_requisicoes}
- Registros extraídos: {total_registros:,}
- Tabela atualizada: eaf_tvro.macro_chatbot

🤖 TOP 10 CATEGORIAS:
"""
                
                for i, (categoria, quantidade) in enumerate(categorias_por_nome.head(10).items(), 1):
                    corpo_email += f"{i}. {categoria}: {quantidade:,} registros\n"

                corpo_email += f"""

🔍 DETALHES TÉCNICOS:
- Conexão SQL Server: {DBSERVER}
- Database: {DATABASE}
- Schema: eaf_tvro
- Método: Substituição completa da tabela (REPLACE)
- Performance: Inserção em chunks de 10.000 registros

📎 ANEXOS:
- Arquivo Excel com todos os dados do chatbot

✅ Status: Processo executado com sucesso!
"""

                msg.set_content(corpo_email)

                # Anexa o arquivo Excel
                with open(excel_filename, "rb") as f:
                    excel_content = f.read()
                    msg.add_attachment(
                        excel_content,
                        maintype="application",
                        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=excel_filename,
                    )

                with smtplib.SMTP("smtp.office365.com", 587) as smtp:
                    smtp.starttls()
                    smtp.login(EMAIL_REMETENTE, SENHA_EMAIL)
                    smtp.send_message(msg)

                logger.info("✅ E-mail de relatório enviado com sucesso!")
            except Exception as e:
                logger.error(f"❌ Erro ao enviar e-mail: {e}")

        # Envia o e-mail de sucesso
        enviar_email_relatorio()

        logger.info("🤖 Extrator de Chatbot Macro finalizado!")

    except Exception as e:
        logger.error(f"❌ Erro no processo: {e}")
        
        # Envia e-mail de erro
        try:
            msg = EmailMessage()
            msg["Subject"] = f"❌ ERRO - Extrator Chatbot Macro {data_execucao.strftime('%d/%m/%Y %H:%M')}"
            msg["From"] = EMAIL_REMETENTE
            msg["To"] = "marcelo.ferreira.terceirizado@eaf.org.br"
            
            corpo_email = f"""
Prezados,

ERRO no Extrator de Chatbot Macro.

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
chatbot_extrator_task = PythonVirtualenvOperator(
    task_id="chatbot_macro_extrator",
    python_callable=main,
    requirements=["pandas==1.5.3", "requests==2.31.0", "sqlalchemy==1.4.49", "pyodbc==4.0.39", "openpyxl==3.1.2"],
    system_site_packages=True,
    dag=dag,
)

chatbot_extrator_task