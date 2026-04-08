"""
DAG: onfly_etl
Descrição: Extrai dados da API Onfly, transforma e carrega no SQL Server.
Cada endpoint/informação é uma task independente, com tratamento de erros,
retries automáticos do Airflow e logging estruturado.

Fluxo:
    autenticar
        ├── extract_transform_colaboradores   -> load_colaboradores
        ├── extract_transform_centro_custo    -> load_centro_custo
        ├── extract_transform_grupo           -> load_grupo
        ├── extract_transform_despesa         -> load_despesa
        ├── extract_transform_automovel       -> load_automovel
        ├── extract_transform_aereo           -> load_aereo
        ├── extract_transform_onibus          -> load_onibus
        ├── extract_transform_hotel           -> load_hotel
        ├── extract_transform_fatura          -> load_fatura
        └── extract_transform_creditos        -> load_creditos
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
TMP_DIR = "/tmp/onfly"
os.makedirs(TMP_DIR, exist_ok=True)

BASE_URL = "https://api.onfly.com.br"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Funções utilitárias (não são tasks) — paginação com retry/backoff
# ---------------------------------------------------------------------------
def fetch_pagina(url: str, headers: dict, pagina: int, max_tentativas: int = 10) -> dict | None:
    """Faz a requisição com retry e backoff exponencial."""
    for tentativa in range(1, max_tentativas + 1):
        try:
            resposta = requests.get(
                url, headers=headers, params={"page": pagina}, timeout=60
            )
            if resposta.status_code == 200:
                try:
                    return resposta.json()
                except ValueError:
                    logger.warning(
                        "Página %s: resposta não é JSON válido. Tentativa %s",
                        pagina, tentativa,
                    )
                    time.sleep(5 * tentativa)
                    continue
            elif resposta.status_code == 429:
                espera = 15 * tentativa
                logger.warning(
                    "Rate limit na página %s. Aguardando %ss (tentativa %s)",
                    pagina, espera, tentativa,
                )
                time.sleep(espera)
            elif resposta.status_code >= 500:
                espera = 10 * tentativa
                logger.warning(
                    "Erro %s na página %s. Aguardando %ss (tentativa %s)",
                    resposta.status_code, pagina, espera, tentativa,
                )
                time.sleep(espera)
            else:
                logger.error(
                    "Erro %s na página %s: %s",
                    resposta.status_code, pagina, resposta.text[:200],
                )
                return None
        except requests.exceptions.RequestException as e:
            espera = 10 * tentativa
            logger.warning(
                "Exceção na página %s: %s. Aguardando %ss (tentativa %s)",
                pagina, e, espera, tentativa,
            )
            time.sleep(espera)
    logger.error("Página %s falhou após %s tentativas", pagina, max_tentativas)
    return None


def coletar_todas_paginas(
    url: str, headers: dict, nome_endpoint: str = "endpoint", pausa: float = 0.3
) -> list[dict]:
    """
    Percorre TODAS as páginas de um endpoint da API Onfly.
    Levanta AirflowException se a primeira página falhar ou se alguma
    página ficar sem ser coletada (dados incompletos não devem ir pro banco).
    """
    logger.info("=== Coletando: %s ===", nome_endpoint)

    primeira = fetch_pagina(url, headers, 1)
    if primeira is None or "meta" not in primeira:
        raise AirflowException(
            f"Falha ao obter a primeira página de {nome_endpoint}. "
            f"Verifique o token, a URL e a conectividade."
        )

    total_paginas = primeira["meta"]["pagination"]["total_pages"]
    total_esperado = primeira["meta"]["pagination"].get("total", 0)
    logger.info(
        "Total de páginas: %s | Registros esperados: %s",
        total_paginas, total_esperado,
    )

    todos_registros: list[dict] = []
    paginas_falhadas: list[int] = []

    for pagina in range(1, total_paginas + 1):
        resultado = primeira if pagina == 1 else fetch_pagina(url, headers, pagina)

        if resultado is None or "data" not in resultado:
            logger.warning("Página %s marcada para retry final", pagina)
            paginas_falhadas.append(pagina)
            continue

        todos_registros.extend(resultado["data"])
        logger.info(
            "Página %s/%s processada (%s registros)",
            pagina, total_paginas, len(resultado["data"]),
        )
        time.sleep(pausa)

    # Retry final
    if paginas_falhadas:
        logger.info(
            "Retry final de %s página(s) que falharam", len(paginas_falhadas)
        )
        ainda_falhadas: list[int] = []
        for pagina in paginas_falhadas:
            time.sleep(5)
            resultado = fetch_pagina(url, headers, pagina, max_tentativas=15)
            if resultado is None or "data" not in resultado:
                ainda_falhadas.append(pagina)
                continue
            todos_registros.extend(resultado["data"])
            logger.info(
                "Página %s recuperada (%s registros)",
                pagina, len(resultado["data"]),
            )

        if ainda_falhadas:
            raise AirflowException(
                f"[{nome_endpoint}] {len(ainda_falhadas)} página(s) NÃO "
                f"foram coletadas: {ainda_falhadas}. Abortando para não "
                f"carregar dados incompletos."
            )

    logger.info(
        "Total coletado: %s registros (esperado: %s)",
        len(todos_registros), total_esperado,
    )
    return todos_registros


def _get_headers(token: str) -> dict:
    return {"Authorization": token}


def _parquet_path(nome: str) -> str:
    return os.path.join(TMP_DIR, f"{nome}.parquet")


def _salvar_df(df: pd.DataFrame, nome: str) -> str:
    """Salva DataFrame em parquet e retorna caminho. Usado para trafegar
    entre tasks sem sobrecarregar o XCom."""
    if df is None or df.empty:
        raise AirflowException(f"DataFrame {nome} está vazio. Abortando.")
    caminho = _parquet_path(nome)
    df_safe = df.copy()
    for col in df_safe.columns:
        if df_safe[col].dtype == object:
            # Substitui NaN por None para evitar a string "nan" no banco
            df_safe[col] = df_safe[col].where(df_safe[col].notna(), None)
    df_safe.to_parquet(caminho, index=False)
    logger.info("DataFrame %s salvo em %s (%s linhas)", nome, caminho, len(df_safe))
    return caminho


def _carregar_sql(nome: str, tabela: str) -> int:
    """Lê o parquet do endpoint e grava no SQL Server."""
    caminho = _parquet_path(nome)
    if not os.path.exists(caminho):
        raise AirflowException(f"Parquet não encontrado: {caminho}")

    df = pd.read_parquet(caminho)
    if df.empty:
        raise AirflowException(f"DataFrame {nome} lido do parquet está vazio")

    # Credenciais obtidas exclusivamente de Variables (mais seguro que os.getenv)
    server = Variable.get("DBSERVER")
    database = Variable.get("DATABASE")
    username = Variable.get("DBUSER")
    password = Variable.get("DBPASSWORD")
    # Porta configurável (padrão 1433)
    porta = Variable.get("DBPORT", default_var="1433")

    if not all([server, database, username, password]):
        raise AirflowException(
            "Credenciais do SQL Server ausentes (DBSERVER/DATABASE/DBUSER/DBPASSWORD)"
        )

    try:
        engine = create_engine(
            f"mssql+pyodbc://{username}:{password}@{server}:{porta}/{database}"
            f"?driver=ODBC Driver 18 for SQL Server"
        )
        df.to_sql(tabela, engine, if_exists="replace", index=False, chunksize=1000)
    except Exception as e:
        raise AirflowException(f"Falha ao gravar tabela {tabela}: {e}") from e

    logger.info("Carregou %s linhas em %s", len(df), tabela)
    return len(df)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="onfly_etl",
    description="ETL dos dados da API Onfly para SQL Server",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",  # todo dia às 06:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["onfly", "api", "sqlserver"],
)
def onfly_etl():

    # -----------------------------------------------------------------------
    # 1. Autenticação
    # -----------------------------------------------------------------------
    @task(retries=5, retry_delay=timedelta(minutes=2))
    def autenticar() -> str:
        # Credenciais obtidas de Variables (mais seguro)
        client_id = Variable.get("onfly_client_id")
        client_secret = Variable.get("onfly_client_secret")

        if not client_id or not client_secret:
            raise AirflowException("client_id/client_secret não configurados")

        payload = {
            "grant_type": "client_credentials",
            "scope": "*",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        try:
            r = requests.post(f"{BASE_URL}/oauth/token", data=payload, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Falha na autenticação Onfly: {e}") from e

        token = r.json().get("access_token")
        if not token:
            raise AirflowException("access_token não retornado pela API Onfly")

        logger.info("Autenticado com sucesso na Onfly")
        return token

    # -----------------------------------------------------------------------
    # 2. Colaboradores
    # -----------------------------------------------------------------------
    @task
    def extract_transform_colaboradores(token: str) -> str:
        headers = _get_headers(token)
        url = f"{BASE_URL}/employees?include=document"
        registros = coletar_todas_paginas(url, headers, "colaboradores")

        lista = []
        for c in registros:
            pos = c.get("position") or {}
            cc = c.get("costCenter") or {}
            doc = (c.get("document") or {}).get("data") or {}
            doc_type = doc.get("type") or {}
            all_docs = ((c.get("allDocuments") or {}).get("data")) or []
            all_docs_str = "; ".join(
                f"{(d.get('type') or {}).get('label', '')}:{d.get('label', '')}"
                for d in all_docs
            )

            lista.append({
                "id": c.get("id"), "name": c.get("name"),
                "username": c.get("username"), "email": c.get("email"),
                "gender": c.get("gender"), "role": c.get("role"),
                "position_id": pos.get("id"), "position_label": pos.get("label"),
                "firstLogin": c.get("firstLogin"), "groupId": c.get("groupId"),
                "language": c.get("language"), "permission": c.get("permission"),
                "birthday": c.get("birthday"), "costCenterId": c.get("costCenterId"),
                "integrationMetadata": c.get("integrationMetadata"),
                "onVacation": c.get("onVacation"), "hasPin": c.get("hasPin"),
                "active": c.get("active"), "parentEmail": c.get("parentEmail"),
                "phoneNumber": c.get("phoneNumber"), "avatar": c.get("avatar"),
                "isPasswordTemporary": c.get("isPasswordTemporary"),
                "hasPhysicalBlueCard": c.get("hasPhysicalBlueCard"),
                "hasLoginRestriction": c.get("hasLoginRestriction"),
                "hasApp": c.get("hasApp"),
                "createdAt": c.get("createdAt"), "updatedAt": c.get("updatedAt"),
                "typeKey": c.get("typeKey"), "keyPix": c.get("keyPix"),
                "acceptedTermLgpd": c.get("acceptedTermLgpd"),
                "costCenter_id": cc.get("id"), "costCenter_name": cc.get("name"),
                "costCenter_customFieldMsSelectOptionId": cc.get("customFieldMsSelectOptionId"),
                "costCenter_integrationMetadata": cc.get("integrationMetadata"),
                "costCenter_deletedAt": cc.get("deletedAt"),
                "costCenter_createdAt": cc.get("createdAt"),
                "costCenter_updatedAt": cc.get("updatedAt"),
                "document_id": doc.get("id"), "document_number": doc.get("number"),
                "document_label": doc.get("label"),
                "document_type_id": doc_type.get("id"),
                "document_type_label": doc_type.get("label"),
                "document_createdAt": doc.get("createdAt"),
                "document_updatedAt": doc.get("updatedAt"),
                "allDocuments": all_docs_str,
            })

        df = pd.DataFrame(lista)
        return _salvar_df(df, "colaboradores")

    @task
    def load_colaboradores(_: str) -> int:
        return _carregar_sql("colaboradores", "colaboradores_novo")

    # -----------------------------------------------------------------------
    # 3. Centro de custo
    # -----------------------------------------------------------------------
    @task
    def extract_transform_centro_custo(token: str) -> str:
        headers = _get_headers(token)
        registros = coletar_todas_paginas(
            f"{BASE_URL}/settings/cost-center", headers, "centro de custo"
        )
        df = pd.DataFrame([{
            "id": cc.get("id"), "v3Id": cc.get("v3Id"), "name": cc.get("name"),
            "integrationMetadata": cc.get("integrationMetadata"),
            "deletedAt": cc.get("deletedAt"),
            "invoicing_email": cc.get("invoicing_email"),
            "invoicing_cnpj": cc.get("invoicing_cnpj"),
            "createdAt": cc.get("createdAt"), "updatedAt": cc.get("updatedAt"),
        } for cc in registros])
        return _salvar_df(df, "centro_custo")

    @task
    def load_centro_custo(_: str) -> int:
        return _carregar_sql("centro_custo", "centro_custo_novo")

    # -----------------------------------------------------------------------
    # 4. Grupos
    # -----------------------------------------------------------------------
    @task
    def extract_transform_grupo(token: str) -> str:
        headers = _get_headers(token)
        registros = coletar_todas_paginas(
            f"{BASE_URL}/employee-groups", headers, "grupos"
        )
        df = pd.DataFrame([{
            "id": g.get("id"), "name": g.get("name"),
            "employeesIds": g.get("employeesIds") or [],
            "amountEmployees": g.get("amountEmployees"),
            "createdAt": g.get("createdAt"), "updatedAt": g.get("updatedAt"),
        } for g in registros]).explode("employeesIds")
        return _salvar_df(df, "grupo")

    @task
    def load_grupo(_: str) -> int:
        return _carregar_sql("grupo", "grupo_novo")

    # -----------------------------------------------------------------------
    # 5. Despesas
    # -----------------------------------------------------------------------
    @task
    def extract_transform_despesa(token: str) -> str:
        headers = _get_headers(token)
        registros = coletar_todas_paginas(
            f"{BASE_URL}/expense/expenditure", headers, "despesas"
        )
        lista = [{
            "id": d.get("id"), "protocol": d.get("protocol"),
            "description": d.get("description"), "date": d.get("date"),
            "amount": d.get("amount"), "currency": d.get("currency"),
            "originalCurrencyAmount": d.get("originalCurrencyAmount"),
            "defaultCurrencyCompany": d.get("defaultCurrencyCompany"),
            "isReimbursable": d.get("isReimbursable"),
            "receipts": ", ".join(d.get("receipts") or []),
            "rdvId": d.get("rdvId"), "costCenterId": d.get("costCenterId"),
            "isValid": d.get("isValid"), "userId": d.get("userId"),
            "expenditureTypeId": d.get("expenditureTypeId"),
            "pixId": d.get("pixId"), "isNFe": d.get("isNFe"),
            "accessKey": d.get("accessKey"), "category": d.get("category"),
            "mcc": d.get("mcc"), "postalCode": d.get("postalCode"),
            "commerce_lat": d.get("commerce_lat"),
            "commerce_lng": d.get("commerce_lng"),
            "commerce_document": d.get("commerce_document"),
            "updatedByBlue": d.get("updatedByBlue"),
            "quoteHasModified": d.get("quoteHasModified"),
            "nfeUri": d.get("nfeUri"), "nfeUF": d.get("nfeUF"),
            "establishmentAddress": d.get("establishmentAddress"),
            "merchant": d.get("merchant"),
            "afterUpdateBlueValue": d.get("afterUpdateBlueValue"),
            "beforeUpdateBlueValue": d.get("beforeUpdateBlueValue"),
            "integrationMetadata": d.get("integrationMetadata"),
            "quantity": d.get("quantity"), "blueMsId": d.get("blueMsId"),
            "isAnnexRequired": d.get("isAnnexRequired"),
            "tagsIds": ", ".join(map(str, d.get("tagsIds") or [])),
            "createdAt": d.get("createdAt"), "updatedAt": d.get("updatedAt"),
        } for d in registros]

        df = pd.DataFrame(lista)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce") / 100
        df["afterUpdateBlueValue"] = pd.to_numeric(df["afterUpdateBlueValue"], errors="coerce") / 100
        df["beforeUpdateBlueValue"] = pd.to_numeric(df["beforeUpdateBlueValue"], errors="coerce") / 100
        df["protocol"] = df["protocol"].astype(str).str.replace("#", "", regex=False)
        return _salvar_df(df, "despesa")

    @task
    def load_despesa(_: str) -> int:
        return _carregar_sql("despesa", "despesa_novo")

    # -----------------------------------------------------------------------
    # 6. Automóveis
    # -----------------------------------------------------------------------
    @task
    def extract_transform_automovel(token: str) -> str:
        headers = _get_headers(token)
        url = f"{BASE_URL}/travel/order/auto-order?include=drivers.user,client"
        registros = coletar_todas_paginas(url, headers, "automóveis")

        lista = []
        for a in registros:
            client = (a.get("client") or {}).get("data") or {}
            withdraw = a.get("withdraw") or {}
            deposit = a.get("deposit") or {}
            drivers = ((a.get("drivers") or {}).get("data")) or []
            drivers_nomes = "; ".join(str(d.get("name") or "") for d in drivers)
            drivers_emails = "; ".join(str(d.get("email") or "") for d in drivers)
            drivers_documentos = "; ".join(str(d.get("document") or "") for d in drivers)
            drivers_user_ids = "; ".join(str(d.get("userId") or "") for d in drivers)

            lista.append({
                "id": a.get("id"), "oldId": a.get("oldId"),
                "protocol": a.get("protocol"), "bookingId": a.get("bookingId"),
                "bookingType": a.get("bookingType"), "status": a.get("status"),
                "labelStatus": a.get("labelStatus"), "reason": a.get("reason"),
                "costCenterId": a.get("costCenterId"),
                "tagsIds": ", ".join(map(str, a.get("tagsIds") or [])),
                "cancelReason": a.get("cancelReason"), "consumer": a.get("consumer"),
                "discount": a.get("discount"), "refunds": a.get("refunds"),
                "additionalCharge": a.get("additionalCharge"),
                "antecedence": a.get("antecedence"),
                "paymentMethod": a.get("paymentMethod"),
                "amount": a.get("amount"), "netAmount": a.get("netAmount"),
                "consumersQuantity": a.get("consumersQuantity"),
                "integrationMetadata": a.get("integrationMetadata"),
                "reprovedReason": a.get("reprovedReason"),
                "approvalExpirationDate": a.get("approvalExpirationDate"),
                "withinPolitics": a.get("withinPolitics"),
                "noHasInvoice": a.get("noHasInvoice"),
                "createdAt": a.get("createdAt"), "updatedAt": a.get("updatedAt"),
                "reservedBy": a.get("reservedBy"), "slug": a.get("slug"),
                "orderCreatedByReqOffline": a.get("orderCreatedByReqOffline"),
                "withdraw_date": withdraw.get("date"),
                "withdraw_city": withdraw.get("city"),
                "withdraw_address": withdraw.get("address"),
                "deposit_date": deposit.get("date"),
                "deposit_city": deposit.get("city"),
                "deposit_address": deposit.get("address"),
                "depositInAnotherLocation": a.get("depositInAnotherLocation"),
                "group": a.get("group"), "description": a.get("description"),
                "renter": a.get("renter"),
                "allowanceDescription": a.get("allowanceDescription"),
                "amountPerDay": a.get("amountPerDay"),
                "totalDays": a.get("totalDays"),
                "dailyAmount": a.get("dailyAmount"),
                "rateDepositAnotherPlace": a.get("rateDepositAnotherPlace"),
                "withdrawObservation": a.get("withdrawObservation"),
                "depositObservation": a.get("depositObservation"),
                "usedAgreement": a.get("usedAgreement"),
                "corporateNameOfAgreement": a.get("corporateNameOfAgreement"),
                "reserveCode": a.get("reserveCode"),
                "bookingDays": a.get("bookingDays"),
                "client_id": client.get("id"), "client_name": client.get("name"),
                "client_email": client.get("email"),
                "client_username": client.get("username"),
                "client_birthday": client.get("birthday"),
                "client_costCenterId": client.get("costCenterId"),
                "client_groupId": client.get("groupId"),
                "client_permission": client.get("permission"),
                "client_active": client.get("active"),
                "client_language": client.get("language"),
                "client_createdAt": client.get("createdAt"),
                "client_updatedAt": client.get("updatedAt"),
                "drivers_nomes": drivers_nomes,
                "drivers_emails": drivers_emails,
                "drivers_documentos": drivers_documentos,
                "drivers_userIds": drivers_user_ids,
            })

        df = pd.DataFrame(lista)
        for col in ["amount", "netAmount", "amountPerDay", "dailyAmount", "refunds"]:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100
        df["protocol"] = df["protocol"].astype(str).str.replace("#", "", regex=False)
        return _salvar_df(df, "automovel")

    @task
    def load_automovel(_: str) -> int:
        return _carregar_sql("automovel", "automovel_novo")

    # -----------------------------------------------------------------------
    # 7. Viagens aéreas
    # -----------------------------------------------------------------------
    @task
    def extract_transform_aereo(token: str) -> str:
        headers = _get_headers(token)
        url = f"{BASE_URL}/travel/order/fly-order/?include=travellers"
        registros = coletar_todas_paginas(url, headers, "viagens aéreas")

        lista = []
        for v in registros:
            outbound = v.get("outbound") or {}
            outbound_cia = outbound.get("cia") or {}
            inbound = v.get("inbound") or {}
            inbound_cia = inbound.get("cia") or {}

            tickets_out = v.get("ticketOutbound") or []
            tickets_in = v.get("ticketInbound") or []
            tickets_out_str = "; ".join(str(t.get("ticket") or "") for t in tickets_out)
            tickets_in_str = "; ".join(str(t.get("ticket") or "") for t in tickets_in)

            travellers = ((v.get("travellers") or {}).get("data")) or []
            trav_nomes = "; ".join(str(t.get("name") or "") for t in travellers)
            trav_emails = "; ".join(str(t.get("email") or "") for t in travellers)
            trav_documentos = "; ".join(str(t.get("document") or "") for t in travellers)
            trav_user_ids = "; ".join(str(t.get("userId") or "") for t in travellers)
            trav_best_price = "; ".join(str(t.get("bestPrice") or "") for t in travellers)
            trav_nationalities = "; ".join(str(t.get("nationality") or "") for t in travellers)

            lista.append({
                "id": v.get("id"), "oldId": v.get("oldId"),
                "protocol": v.get("protocol"), "bookingId": v.get("bookingId"),
                "bookingType": v.get("bookingType"), "status": v.get("status"),
                "labelStatus": v.get("labelStatus"), "reason": v.get("reason"),
                "costCenterId": v.get("costCenterId"),
                "tagsIds": ", ".join(map(str, v.get("tagsIds") or [])),
                "cancelReason": v.get("cancelReason"), "consumer": v.get("consumer"),
                "discount": v.get("discount"), "refunds": v.get("refunds"),
                "additionalCharge": v.get("additionalCharge"),
                "antecedence": v.get("antecedence"),
                "paymentMethod": v.get("paymentMethod"),
                "amount": v.get("amount"), "netAmount": v.get("netAmount"),
                "consumersQuantity": v.get("consumersQuantity"),
                "integrationMetadata": v.get("integrationMetadata"),
                "reprovedReason": v.get("reprovedReason"),
                "approvalExpirationDate": v.get("approvalExpirationDate"),
                "withinPolitics": v.get("withinPolitics"),
                "noHasInvoice": v.get("noHasInvoice"),
                "createdAt": v.get("createdAt"), "updatedAt": v.get("updatedAt"),
                "reservedBy": v.get("reservedBy"), "slug": v.get("slug"),
                "orderCreatedByReqOffline": v.get("orderCreatedByReqOffline"),
                "type": v.get("type"), "isRoundTrip": v.get("isRoundTrip"),
                "creditRefunds": v.get("creditRefunds"),
                "hasBag": v.get("hasBag"), "bagAmount": v.get("bagAmount"),
                "hasComfortSeat": v.get("hasComfortSeat"),
                "comfortSeatAmount": v.get("comfortSeatAmount"),
                "hasInsurance": v.get("hasInsurance"),
                "insuranceAmount": v.get("insuranceAmount"),
                "hasAutoCheckin": v.get("hasAutoCheckin"),
                "autoCheckinAmount": v.get("autoCheckinAmount"),
                "isInternational": v.get("isInternational"),
                "totalEmissionGrams": v.get("totalEmissionGrams"),
                "ticketOutbound": tickets_out_str, "ticketInbound": tickets_in_str,
                "outbound_from": outbound.get("from"),
                "outbound_to": outbound.get("to"),
                "outbound_departureDate": outbound.get("departureDate"),
                "outbound_arrivalDate": outbound.get("arrivalDate"),
                "outbound_tax": outbound.get("tax"),
                "outbound_flightNumber": outbound.get("flightNumber"),
                "outbound_cabinType": outbound.get("cabinType"),
                "outbound_hasStops": outbound.get("hasStops"),
                "outbound_duration": outbound.get("duration"),
                "outbound_cabinBusinessClass": outbound.get("cabinBusinessClass"),
                "outbound_emissionGrams": outbound.get("emissionGrams"),
                "outbound_cia_id": outbound_cia.get("id"),
                "outbound_cia_name": outbound_cia.get("name"),
                "outbound_cia_iata": outbound_cia.get("iata"),
                "outbound_cia_callSign": outbound_cia.get("callSign"),
                "outbound_cia_status": outbound_cia.get("status"),
                "inbound_from": inbound.get("from"),
                "inbound_to": inbound.get("to"),
                "inbound_departureDate": inbound.get("departureDate"),
                "inbound_arrivalDate": inbound.get("arrivalDate"),
                "inbound_tax": inbound.get("tax"),
                "inbound_flightNumber": inbound.get("flightNumber"),
                "inbound_cabinType": inbound.get("cabinType"),
                "inbound_hasStops": inbound.get("hasStops"),
                "inbound_duration": inbound.get("duration"),
                "inbound_cabinBusinessClass": inbound.get("cabinBusinessClass"),
                "inbound_emissionGrams": inbound.get("emissionGrams"),
                "inbound_cia_id": inbound_cia.get("id"),
                "inbound_cia_name": inbound_cia.get("name"),
                "inbound_cia_iata": inbound_cia.get("iata"),
                "inbound_cia_callSign": inbound_cia.get("callSign"),
                "inbound_cia_status": inbound_cia.get("status"),
                "travellers_nomes": trav_nomes,
                "travellers_emails": trav_emails,
                "travellers_documentos": trav_documentos,
                "travellers_userIds": trav_user_ids,
                "travellers_bestPrice": trav_best_price,
                "travellers_nationalities": trav_nationalities,
            })

        df = pd.DataFrame(lista)
        cols_divide = [
            "amount", "netAmount", "refunds", "discount", "additionalCharge",
            "bagAmount", "comfortSeatAmount", "insuranceAmount",
            "autoCheckinAmount", "outbound_tax", "inbound_tax",
        ]
        for col in cols_divide:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100
        df["protocol"] = df["protocol"].astype(str).str.replace("#", "", regex=False)
        return _salvar_df(df, "aereo")

    @task
    def load_aereo(_: str) -> int:
        # CORREÇÃO: nome da tabela corrigido para "aereo_novo"
        return _carregar_sql("aereo", "aereo_novo")

    # -----------------------------------------------------------------------
    # 8. Ônibus
    # -----------------------------------------------------------------------
    @task
    def extract_transform_onibus(token: str) -> str:
        headers = _get_headers(token)
        url = f"{BASE_URL}/travel/order/bus-order?include=client"
        registros = coletar_todas_paginas(url, headers, "ônibus")

        lista = []
        for o in registros:
            client = (o.get("client") or {}).get("data") or {}
            outbound = o.get("outbound") or {}
            outbound_from = outbound.get("from") or {}
            outbound_to = outbound.get("to") or {}
            outbound_cia = outbound.get("cia") or {}
            inbound = o.get("inbound") or {}
            inbound_from = inbound.get("from") or {}
            inbound_to = inbound.get("to") or {}
            inbound_cia = inbound.get("cia") or {}

            lista.append({
                "id": o.get("id"), "oldId": o.get("oldId"),
                "protocol": o.get("protocol"), "bookingId": o.get("bookingId"),
                "bookingType": o.get("bookingType"), "status": o.get("status"),
                "labelStatus": o.get("labelStatus"), "reason": o.get("reason"),
                "costCenterId": o.get("costCenterId"),
                "tagsIds": ", ".join(map(str, o.get("tagsIds") or [])),
                "cancelReason": o.get("cancelReason"), "consumer": o.get("consumer"),
                "discount": o.get("discount"), "refunds": o.get("refunds"),
                "additionalCharge": o.get("additionalCharge"),
                "antecedence": o.get("antecedence"),
                "paymentMethod": o.get("paymentMethod"),
                "amount": o.get("amount"), "netAmount": o.get("netAmount"),
                "consumersQuantity": o.get("consumersQuantity"),
                "integrationMetadata": o.get("integrationMetadata"),
                "reprovedReason": o.get("reprovedReason"),
                "approvalExpirationDate": o.get("approvalExpirationDate"),
                "withinPolitics": o.get("withinPolitics"),
                "noHasInvoice": o.get("noHasInvoice"),
                "createdAt": o.get("createdAt"), "updatedAt": o.get("updatedAt"),
                "reservedBy": o.get("reservedBy"), "slug": o.get("slug"),
                "orderCreatedByReqOffline": o.get("orderCreatedByReqOffline"),
                "isRoundTrip": o.get("isRoundTrip"),
                "hasInsurance": o.get("hasInsurance"),
                "client_id": client.get("id"), "client_name": client.get("name"),
                "client_email": client.get("email"),
                "client_username": client.get("username"),
                "client_birthday": client.get("birthday"),
                "client_costCenterId": client.get("costCenterId"),
                "client_groupId": client.get("groupId"),
                "client_permission": client.get("permission"),
                "client_active": client.get("active"),
                "client_language": client.get("language"),
                "client_createdAt": client.get("createdAt"),
                "client_updatedAt": client.get("updatedAt"),
                "outbound_type": outbound.get("type"),
                "outbound_seatClass": outbound.get("seatClass"),
                "outbound_departureDate": outbound.get("departureDate"),
                "outbound_arrivalDate": outbound.get("arrivalDate"),
                "outbound_price": outbound.get("price"),
                "outbound_tax": outbound.get("tax"),
                "outbound_insurancePrice": outbound.get("insurancePrice"),
                "outbound_refundable": outbound.get("refundable"),
                "outbound_from_id": outbound_from.get("id"),
                "outbound_from_name": outbound_from.get("name"),
                "outbound_from_city": outbound_from.get("city"),
                "outbound_from_station": outbound_from.get("station"),
                "outbound_from_address": outbound_from.get("address"),
                "outbound_to_id": outbound_to.get("id"),
                "outbound_to_name": outbound_to.get("name"),
                "outbound_to_city": outbound_to.get("city"),
                "outbound_to_station": outbound_to.get("station"),
                "outbound_to_address": outbound_to.get("address"),
                "outbound_cia_id": outbound_cia.get("id"),
                "outbound_cia_name": outbound_cia.get("name"),
                "outbound_cia_displayName": outbound_cia.get("displayName"),
                "inbound_type": inbound.get("type"),
                "inbound_seatClass": inbound.get("seatClass"),
                "inbound_departureDate": inbound.get("departureDate"),
                "inbound_arrivalDate": inbound.get("arrivalDate"),
                "inbound_price": inbound.get("price"),
                "inbound_tax": inbound.get("tax"),
                "inbound_insurancePrice": inbound.get("insurancePrice"),
                "inbound_refundable": inbound.get("refundable"),
                "inbound_from_id": inbound_from.get("id"),
                "inbound_from_name": inbound_from.get("name"),
                "inbound_from_city": inbound_from.get("city"),
                "inbound_from_station": inbound_from.get("station"),
                "inbound_from_address": inbound_from.get("address"),
                "inbound_to_id": inbound_to.get("id"),
                "inbound_to_name": inbound_to.get("name"),
                "inbound_to_city": inbound_to.get("city"),
                "inbound_to_station": inbound_to.get("station"),
                "inbound_to_address": inbound_to.get("address"),
                "inbound_cia_id": inbound_cia.get("id"),
                "inbound_cia_name": inbound_cia.get("name"),
                "inbound_cia_displayName": inbound_cia.get("displayName"),
            })

        df = pd.DataFrame(lista)
        cols_divide = [
            "amount", "netAmount", "refunds", "discount", "additionalCharge",
            "outbound_price", "outbound_tax", "outbound_insurancePrice",
            "inbound_price", "inbound_tax", "inbound_insurancePrice",
        ]
        for col in cols_divide:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100
        df["protocol"] = df["protocol"].astype(str).str.replace("#", "", regex=False)
        return _salvar_df(df, "onibus")

    @task
    def load_onibus(_: str) -> int:
        return _carregar_sql("onibus", "onibus_novo")

    # -----------------------------------------------------------------------
    # 9. Hotel
    # -----------------------------------------------------------------------
    @task
    def extract_transform_hotel(token: str) -> str:
        headers = _get_headers(token)
        url = (
            f"{BASE_URL}/travel/order/hotel-order/?include=costCenter,client,"
            f"approvalFlowHistory.changedBy,tags,guests,guests.document,"
            f"guests.user,nextApprovalUsers,chargesAdditional,refundOrders,coupon"
        )
        registros = coletar_todas_paginas(url, headers, "hotel", pausa=0.5)

        lista = []
        for h in registros:
            client = (h.get("client") or {}).get("data") or {}
            cc = (h.get("costCenter") or {}).get("data") or {}
            address = h.get("address") or {}

            tags = (h.get("tags") or {}).get("data") or []
            tags_nomes = "; ".join(str(t.get("name") or "") for t in tags)

            next_approvers = (h.get("nextApprovalUsers") or {}).get("data") or []
            next_approvers_nomes = "; ".join(str(u.get("name") or "") for u in next_approvers)
            next_approvers_emails = "; ".join(str(u.get("email") or "") for u in next_approvers)

            flow = (h.get("approvalFlowHistory") or {}).get("data") or []
            flow_str = "; ".join(
                f"{str(f.get('actionLabel') or '')}({str(f.get('date') or '')})"
                for f in flow
            )

            guests = (h.get("guests") or {}).get("data") or []
            guests_nomes = "; ".join(str(g.get("name") or "") for g in guests)
            guests_emails = "; ".join(str(g.get("email") or "") for g in guests)
            guests_documentos = "; ".join(str(g.get("document") or "") for g in guests)
            guests_user_ids = "; ".join(str(g.get("userId") or "") for g in guests)
            guests_nationalities = "; ".join(str(g.get("nationality") or "") for g in guests)

            lista.append({
                "id": h.get("id"), "oldId": h.get("oldId"),
                "protocol": h.get("protocol"), "bookingId": h.get("bookingId"),
                "bookingType": h.get("bookingType"), "status": h.get("status"),
                "labelStatus": h.get("labelStatus"), "reason": h.get("reason"),
                "costCenterId": h.get("costCenterId"),
                "tagsIds": ", ".join(map(str, h.get("tagsIds") or [])),
                "cancelReason": h.get("cancelReason"), "consumer": h.get("consumer"),
                "discount": h.get("discount"), "refunds": h.get("refunds"),
                "additionalCharge": h.get("additionalCharge"),
                "antecedence": h.get("antecedence"),
                "paymentMethod": h.get("paymentMethod"),
                "amount": h.get("amount"), "netAmount": h.get("netAmount"),
                "consumersQuantity": h.get("consumersQuantity"),
                "integrationMetadata": h.get("integrationMetadata"),
                "reprovedReason": h.get("reprovedReason"),
                "approvalExpirationDate": h.get("approvalExpirationDate"),
                "withinPolitics": h.get("withinPolitics"),
                "noHasInvoice": h.get("noHasInvoice"),
                "createdAt": h.get("createdAt"), "updatedAt": h.get("updatedAt"),
                "reservedBy": h.get("reservedBy"), "slug": h.get("slug"),
                "orderCreatedByReqOffline": h.get("orderCreatedByReqOffline"),
                "checkin": h.get("checkin"), "checkout": h.get("checkout"),
                "hotelName": h.get("hotelName"), "city": h.get("city"),
                "roomDescription": h.get("roomDescription"),
                "allowancesDescription": h.get("allowancesDescription"),
                "thumbnail": h.get("thumbnail"),
                "isRefundable": h.get("isRefundable"),
                "refundDeadline": h.get("refundDeadline"),
                "dailyAmount": h.get("dailyAmount"),
                "bookingDays": h.get("bookingDays"),
                "daysHosted": h.get("daysHosted"),
                "bestPrice": h.get("bestPrice"),
                "isBestPrice": h.get("isBestPrice"),
                "costCenter_id": cc.get("id"), "costCenter_name": cc.get("name"),
                "costCenter_v3Id": cc.get("v3Id"),
                "costCenter_integrationMetadata": cc.get("integrationMetadata"),
                "costCenter_deletedAt": cc.get("deletedAt"),
                "client_id": client.get("id"), "client_name": client.get("name"),
                "client_email": client.get("email"),
                "client_username": client.get("username"),
                "client_birthday": client.get("birthday"),
                "client_costCenterId": client.get("costCenterId"),
                "client_groupId": client.get("groupId"),
                "client_permission": client.get("permission"),
                "client_active": client.get("active"),
                "client_language": client.get("language"),
                "client_createdAt": client.get("createdAt"),
                "client_updatedAt": client.get("updatedAt"),
                "address_streetName": address.get("streetName"),
                "address_streetNumber": address.get("streetNumber"),
                "address_postCode": address.get("postCode"),
                "address_region": address.get("region"),
                "address_district": address.get("district"),
                "address_countryName": address.get("countryName"),
                "address_placeId": address.get("placeId"),
                "address_description": address.get("description"),
                "address_name": address.get("name"),
                "address_type": address.get("type"),
                "address_lat": address.get("lat"),
                "address_lng": address.get("lng"),
                "address_cityName": address.get("cityName"),
                "tags_nomes": tags_nomes,
                "nextApprovers_nomes": next_approvers_nomes,
                "nextApprovers_emails": next_approvers_emails,
                "approvalFlowHistory": flow_str,
                "guests_nomes": guests_nomes,
                "guests_emails": guests_emails,
                "guests_documentos": guests_documentos,
                "guests_userIds": guests_user_ids,
                "guests_nationalities": guests_nationalities,
            })

        df = pd.DataFrame(lista)
        cols_divide = [
            "amount", "netAmount", "refunds", "discount",
            "additionalCharge", "dailyAmount", "bestPrice",
        ]
        for col in cols_divide:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100
        df["protocol"] = df["protocol"].astype(str).str.replace("#", "", regex=False)
        return _salvar_df(df, "hotel")

    @task
    def load_hotel(_: str) -> int:
        return _carregar_sql("hotel", "hotel_novo")

    # -----------------------------------------------------------------------
    # 10. Fatura
    # -----------------------------------------------------------------------
    @task
    def extract_transform_fatura(token: str) -> str:
        headers = _get_headers(token)
        # CORREÇÃO: typo "iclude" corrigido para "include"
        url = f"{BASE_URL}/financial/invoice/list/invoice?include=details"
        registros = coletar_todas_paginas(url, headers, "fatura")

        df = pd.DataFrame(registros)
        if "amount" in df.columns:
            # CORREÇÃO: manter como float, sem formatar para string
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce") / 100
        df.drop(columns=["description", "invoicedCompany"], inplace=True, errors="ignore")
        return _salvar_df(df, "fatura")

    @task
    def load_fatura(_: str) -> int:
        return _carregar_sql("fatura", "fatura_novo")

    # -----------------------------------------------------------------------
    # 11. Créditos
    # -----------------------------------------------------------------------
    @task
    def extract_transform_creditos(token: str) -> str:
        headers = _get_headers(token)
        url = f"{BASE_URL}/credits/groupByConsumer"
        registros = coletar_todas_paginas(url, headers, "créditos")

        df = pd.json_normalize(registros)

        if "credits.data" in df.columns:
            df["credits.data"] = df["credits.data"].apply(
                lambda x: x if isinstance(x, list) else []
            )
            df_explodido = df.explode("credits.data").reset_index(drop=True)
            df_credits = pd.json_normalize(df_explodido["credits.data"])
            df_final = pd.concat(
                [
                    df_explodido.drop(columns=["credits.data"]),
                    df_credits.add_prefix("credit.").reset_index(drop=True),
                ],
                axis=1,
            )
        else:
            df_final = df.copy()

        if "credit.data" in df_final.columns:
            df_final["credit.data"] = df_final["credit.data"].apply(
                lambda x: x if isinstance(x, list) else []
            )
            df_explodido2 = df_final.explode("credit.data").reset_index(drop=True)
            df_data = pd.json_normalize(df_explodido2["credit.data"])
            df_final = pd.concat(
                [
                    df_explodido2.drop(columns=["credit.data"]),
                    df_data.add_prefix("credit_data.").reset_index(drop=True),
                ],
                axis=1,
            )

        colunas_desejadas = [
            "id", "name", "createdAt", "updatedAt",
            "credit.id", "credit.totalAmount",
            "credit.description", "credit.user",
        ]
        colunas_existentes = [c for c in colunas_desejadas if c in df_final.columns]
        df_creditos = df_final[colunas_existentes]
        return _salvar_df(df_creditos, "creditos")

    @task
    def load_creditos(_: str) -> int:
        return _carregar_sql("creditos", "creditos_novo")

    # -----------------------------------------------------------------------
    # Encadeamento
    # -----------------------------------------------------------------------
    token = autenticar()

    # Cada endpoint é uma extração independente seguida da carga.
    load_colaboradores(extract_transform_colaboradores(token))
    load_centro_custo(extract_transform_centro_custo(token))
    load_grupo(extract_transform_grupo(token))
    load_despesa(extract_transform_despesa(token))
    load_automovel(extract_transform_automovel(token))
    load_aereo(extract_transform_aereo(token))
    load_onibus(extract_transform_onibus(token))
    load_hotel(extract_transform_hotel(token))
    load_fatura(extract_transform_fatura(token))
    load_creditos(extract_transform_creditos(token))


dag_instance = onfly_etl()