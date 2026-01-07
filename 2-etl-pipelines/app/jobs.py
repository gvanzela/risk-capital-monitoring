import requests
import pandas as pd
import re
from datetime import datetime, timezone, timedelta
from time import sleep
from sqlalchemy import text

from app.config import (
    METABASE_BASE,
    METABASE_USER,
    METABASE_PASS,
    METABASE_PUBLIC_CARD_ENDPOINT,
    FUNDS_API_BASE,
    FUNDS_API_ENDPOINT_PL,
    FUNDS_API_COOKIE,
    FUNDS_API_CRYPTO_TOKEN,
    REQUEST_TIMEOUT,
    REQUEST_RETRY_SLEEP,
    VERIFY_SSL
)

from app.db import ENGINE_REMOTE as ENGINE, ENGINE_LOCAL

# ============================================================
# METABASE — SESSION & AUTH
# ============================================================

SESSION = requests.Session()

def metabase_login():
    r = SESSION.post(
        f"{METABASE_BASE}/api/session",
        json={
            "username": METABASE_USER,
            "password": METABASE_PASS
        },
        timeout=REQUEST_TIMEOUT,
        verify=VERIFY_SSL
    )
    r.raise_for_status()

def mb_request(method, url, **kwargs):
    r = SESSION.request(method, url, **kwargs)

    if r.status_code == 401:
        metabase_login()
        r = SESSION.request(method, url, **kwargs)

    r.raise_for_status()
    return r

# ============================================================
# JOB — MANAGER MARGIN SNAPSHOT
# ============================================================
# Purpose:
# - Retrieve daily margin data per manager from Metabase
# - Normalize data types
# - Persist snapshot into MySQL
#
# Source:
# - Metabase public card (no auth header required)
# ============================================================

def run_margem(log):
    log("Starting Manager Margin job...")

    # --------------------------------------------------------
    # Build Metabase public card URL
    # --------------------------------------------------------
    url = (
        f"{METABASE_BASE}"
        f"{METABASE_PUBLIC_CARD_ENDPOINT.format(card_id=METABASE_CARD_MARGEM)}"
    )

    # --------------------------------------------------------
    # API request (auto re-login handled by mb_request)
    # --------------------------------------------------------
    try:
        resp = mb_request(
            method="GET",
            url=url,
            timeout=REQUEST_TIMEOUT,
            verify=VERIFY_SSL
        )
        data = resp.json()
    except Exception as e:
        log(f"ERROR — Manager Margin request failed: {e}")
        return

    # --------------------------------------------------------
    # Load into DataFrame
    # --------------------------------------------------------
    df = pd.DataFrame(data)

    if df.empty:
        log("Manager Margin returned no data.")
        return

    # --------------------------------------------------------
    # Data type normalization
    # --------------------------------------------------------
    if "DataEnvio" in df.columns:
        df["DataEnvio"] = pd.to_datetime(df["DataEnvio"], errors="coerce")

    if "MargemLocal" in df.columns:
        df["MargemLocal"] = pd.to_numeric(df["MargemLocal"], errors="coerce")

    if "MargemOffshore" in df.columns:
        df["MargemOffshore"] = pd.to_numeric(df["MargemOffshore"], errors="coerce")

    # --------------------------------------------------------
    # Load timestamp (BRT)
    # --------------------------------------------------------
    tz_brt = timezone(timedelta(hours=-3))
    df["dt_carga"] = datetime.now(tz_brt)

    # --------------------------------------------------------
    # Persist snapshot (append-only)
    # --------------------------------------------------------
    df.to_sql(
        name="TB_ENQ_MARGEM_GESTOR_SNAPSHOT",
        con=ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    log(f"Manager Margin inserted: {len(df)} rows")

# ============================================================
# JOB — PL SNAPSHOT
# ============================================================
# Purpose:
# - Retrieve fund-level PL and risk metrics
# - Expand nested JSON payload
# - Normalize schema
# - Persist daily snapshot into MySQL
#
# Source:
# - Funds Platform internal API
# ============================================================

def run_pl_snapshot(log):
    log("Starting PL Snapshot job...")

    # --------------------------------------------------------
    # Build endpoint
    # --------------------------------------------------------
    url = f"{FUNDS_API_BASE}{FUNDS_API_ENDPOINT_PL}"

    headers = {
        "Cookie": FUNDS_API_COOKIE,
        "x-crypto-token": FUNDS_API_CRYPTO_TOKEN
    }

    # --------------------------------------------------------
    # API request
    # --------------------------------------------------------
    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=VERIFY_SSL
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log(f"ERROR — PL Snapshot request failed: {e}")
        return

    # --------------------------------------------------------
    # Base DataFrame
    # --------------------------------------------------------
    df = pd.DataFrame(data)

    if df.empty:
        log("PL Snapshot returned no data.")
        return

    # --------------------------------------------------------
    # Expand nested 'resultado' payload
    # --------------------------------------------------------
    df_expanded = pd.json_normalize(df["resultado"], sep="_")

    # Remove JSON prefix artifacts
    df_expanded.columns = [
        re.sub(r"^(detalhesJson[._])", "", c) for c in df_expanded.columns
    ]

    # Merge base + expanded
    df_final = pd.concat(
        [df.drop(columns=["resultado"]), df_expanded],
        axis=1
    )

    # --------------------------------------------------------
    # Load timestamp
    # --------------------------------------------------------
    df_final["dt_carga"] = datetime.now()

    # --------------------------------------------------------
    # Enforce schema alignment (only valid columns)
    # --------------------------------------------------------
    colunas_validas = [
        "sucesso", "erros", "cgePortfolio", "dataProcessamento", "dataPosicao",
        "nomeFundo", "cnpj", "cgeGestor", "nomeGestor", "publicoAlvo",
        "tipoPortfolio", "classAnbidScp", "fundoFechado", "restricaoInvestimento",
        "classeCvm", "descClasseCvm", "pl", "margemPl", "contaCorrente",
        "saldoCc", "derivativos", "derivativosRisco", "derivativosPl",
        "derivativosRiscoPl", "liquidezPl", "rentabilidadeDia",
        "rentabilidadeMes", "rentabilidadeAno", "criticidade",
        "var95", "var99", "bull", "bear",
        "bullParis", "bearParis",
        "var95Paris3M", "var95Paris1Y", "var95Paris2Y",
        "var99Paris3M", "var99Paris1Y", "var99Paris2Y",
        "fiiFiq", "dt_carga"
    ]

    df_final = df_final[colunas_validas]

    # --------------------------------------------------------
    # Persist snapshot (append-only)
    # --------------------------------------------------------
    df_final.to_sql(
        name="TB_ENQ_PL_SNAPSHOT",
        con=ENGINE,
        if_exists="append",
        index=False
    )

    log("PL Snapshot completed.")

# ============================================================
# JOB — PL HISTORICAL SNAPSHOT
# ============================================================
# Purpose:
# - Retrieve historical PL for a given reference date
# - Persist daily PL history per fund
#
# Source:
# - Metabase public card with date parameter
# ============================================================

def run_pl_historico(log, data_carteira):
    log(f"Starting PL Historical job ({data_carteira})...")

    # --------------------------------------------------------
    # Build Metabase public card URL with date parameter
    # --------------------------------------------------------
    url = (
        f"{METABASE_BASE}"
        f"{METABASE_PUBLIC_CARD_ENDPOINT.format(card_id=METABASE_CARD_PL_HIST)}"
    )

    params = {
        "parameters": (
            f'[{{"type":"date/single","value":"{data_carteira}",'
            f'"target":["variable",["template-tag","DataCarteira"]]}}]'
        )
    }

    # --------------------------------------------------------
    # API request
    # --------------------------------------------------------
    try:
        resp = mb_request(
            method="GET",
            url=url,
            params=params,
            timeout=REQUEST_TIMEOUT,
            verify=VERIFY_SSL
        )
        data = resp.json()
    except Exception as e:
        log(f"ERROR — PL Historical request failed: {e}")
        return

    # --------------------------------------------------------
    # Load into DataFrame
    # --------------------------------------------------------
    df = pd.DataFrame(data)

    if df.empty:
        log("PL Historical returned no data.")
        return

    # --------------------------------------------------------
    # Data normalization
    # --------------------------------------------------------
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date

    tz_brt = timezone(timedelta(hours=-3))
    df["dt_carga"] = datetime.now(tz_brt)

    df = df.rename(columns={
        "CgePortfolio": "cgePortfolio",
        "Data": "data",
        "PatrimonioAbertura": "patrimonio_abertura",
        "PatrimonioFechamento": "patrimonio_fechamento"
    })

    # --------------------------------------------------------
    # Persist historical data (append-only)
    # --------------------------------------------------------
    df.to_sql(
        name="TB_ENQ_PL_HISTORICO",
        con=ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    log(f"PL Historical completed ({len(df)} rows).")


# ============================================================
# JOB — FUND POSITIONS (OTC / OFFSHORE / SWAPS)
# ============================================================
# Purpose:
# - Identify exposed funds (CGEs) based on PL history
# - Retrieve positions from Metabase (private + public cards)
# - Normalize and consolidate positions
# - Persist snapshot into MySQL
#
# IMPORTANT:
# - Logic preserved exactly as original implementation
# - No behavioral changes, only structural adaptation
# ============================================================

def run_posicoes(log):
    log("Starting Fund Positions job...")

    # =========================================================
    # METABASE ENDPOINTS
    # =========================================================

    METABASE_PRIVATE_CARD_URL = f"{METABASE_BASE}/api/card/{{card_id}}/query/json"

    METABASE_PUBLIC_CARD_URL = (
        f"{METABASE_BASE}"
        f"{METABASE_PUBLIC_CARD_ENDPOINT.format(card_id=METABASE_CARD_POSICOES_PUBLIC)}"
    )

    HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    # =========================================================
    # 1. GLOBAL REFERENCE DATE (PL BASE)
    # =========================================================

    def get_maior_data():
        query = """
            SELECT MAX(data) AS ultima_data
            FROM TB_ENQ_PL_HISTORICO;
        """
        df = pd.read_sql(query, ENGINE)
        return df.iloc[0]["ultima_data"].strftime("%Y-%m-%d")

    # =========================================================
    # 2. PRIVATE METABASE CARD QUERY (ORIGINAL LOGIC)
    # =========================================================

    def consultar_card(card_id, data_ref):
        """
        Original behavior:
        - On error, return DataFrame with column 'CgePortfolio'
        - Prevents KeyError downstream
        """
        url = METABASE_PRIVATE_CARD_URL.format(card_id=card_id)

        body = (
            f'parameters=[{{'
            f'"type":"date/single",'
            f'"value":"{data_ref}",'
            f'"target":["variable",["template-tag","DtCarteira"]],'
            f'"id":"8df24e4e-e7f5-d6ed-1585-bce1ad69569c"'
            f'}}]'
        )

        try:
            resp = mb_request(
                method="POST",
                url=url,
                headers=HEADERS,
                data=body,
                timeout=REQUEST_TIMEOUT,
                verify=VERIFY_SSL
            )
            data = resp.json()
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception as e:
            log(f"WARNING — Card {card_id} failed: {e}")
            return pd.DataFrame(columns=["CgePortfolio"])

    # =========================================================
    # 3. IDENTIFY EXPOSED CGEs (OTC / SWAP / OFF)
    # =========================================================

    data_ref_global = get_maior_data()

    df_otc_full  = consultar_card(METABASE_CARD_POSICOES_OTC,  data_ref_global)
    df_swap_full = consultar_card(METABASE_CARD_POSICOES_SWAP, data_ref_global)
    df_off_full  = consultar_card(METABASE_CARD_POSICOES_OFF,  data_ref_global)

    df_otc  = df_otc_full[["CgePortfolio"]].drop_duplicates()
    df_swap = df_swap_full[["CgePortfolio"]].drop_duplicates()
    df_off  = df_off_full[["CgePortfolio"]].drop_duplicates()

    # Original integrity guard
    if df_otc.empty or df_swap.empty or df_off.empty:
        log("Aborting — one or more cards returned no data (OTC / SWAP / OFF).")
        return

    df_cges = (
        pd.concat([df_otc, df_swap, df_off], ignore_index=True)
          .drop_duplicates()
    )

    log(f"Identified {len(df_cges)} exposed funds.")

    # =========================================================
    # 4. LATEST DATE PER CGE
    # =========================================================

    query_datas = f"""
        SELECT
            CgePortfolio,
            MAX(data) AS ultima_data
        FROM TB_ENQ_PL_HISTORICO
        WHERE CgePortfolio IN (
            {",".join(df_cges["CgePortfolio"].astype(str))}
        )
        GROUP BY CgePortfolio;
    """

    df_datas = pd.read_sql(query_datas, ENGINE)

    # =========================================================
    # 5. PUBLIC CARD — POSITIONS PER CGE
    # =========================================================

    dfs = []

    for _, row in df_datas.iterrows():
        cge = str(int(row["CgePortfolio"]))
        data_carteira = pd.to_datetime(row["ultima_data"]).strftime("%Y-%m-%d")

        params = {
            "parameters": (
                f'[{{'
                f'"type":"date/single",'
                f'"value":"{data_carteira}",'
                f'"target":["variable",["template-tag","Data"]]'
                f'}},'
                f'{{'
                f'"type":"number/=","value":["{cge}"],'
                f'"target":["variable",["template-tag","CGE"]]'
                f'}}]'
            )
        }

        try:
            resp = mb_request(
                method="GET",
                url=METABASE_PUBLIC_CARD_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
                verify=VERIFY_SSL
            )

            data = resp.json()
            if not data:
                continue

            df_tmp = pd.DataFrame(data)
            df_tmp["CgePortfolio"] = cge
            df_tmp["dt_carteira"] = data_carteira

            dfs.append(df_tmp)

        except Exception as e:
            log(f"WARNING — CGE {cge} failed: {e}")

        sleep(REQUEST_RETRY_SLEEP)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if final_df.empty:
        log("No positions returned.")
        return

    # =========================================================
    # 6. DATA NORMALIZATION (ORIGINAL)
    # =========================================================

    final_df["DataCarteira"] = pd.to_datetime(final_df["DataCarteira"], errors="coerce")
    final_df["dt_carteira"] = pd.to_datetime(final_df["dt_carteira"], errors="coerce")

    for col in ["notional", "ValorCotacao", "qtyposicao", "IdClassificacao", "valorfinanceiro"]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

    for col in ["CodAtivo", "CodTipoAtivo", "CgePortfolio"]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce").astype("Int64")

    # =========================================================
    # 7. FINAL LAYOUT (ORIGINAL)
    # =========================================================

    final_df["dt_insercao"] = pd.Timestamp.now()

    cols_final = [
        "Nickname", "DataCarteira", "notional", "CgePortfolio",
        "ValorCotacao", "NmClassificacao", "qtyposicao",
        "IdClassificacao", "valorfinanceiro", "CodAtivo",
        "NuIsin", "CodTipoAtivo", "dt_carteira", "dt_insercao",
        "ValorSaldoAtivoSwap", "ValorSaldoPassivoSwap"
    ]

    for col in cols_final:
        if col not in final_df.columns:
            final_df[col] = None

    final_df = final_df[cols_final]

    # =========================================================
    # 8. CLASSIFICATION FIX (ORIGINAL)
    # =========================================================

    final_df.loc[
        final_df["Nickname"].isin(df_off_full["Nickname"]),
        "NmClassificacao"
    ] = "Fundo Offshore"

    final_df.loc[
        final_df["Nickname"].isin(df_otc_full["Nickname"]),
        "NmClassificacao"
    ] = "OTC OPC"

    # =========================================================
    # 9. PERSIST SNAPSHOT
    # =========================================================

    final_df.to_sql(
        name="TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
        con=ENGINE,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi"
    )

    log("Fund Positions job completed.")

# ============================================================
# AUX — UPDATE RISK EXPOSURE SNAPSHOT
# ============================================================
# Purpose:
# - Rebuild TB_ENQ_EXPOSI_RISCO_SNAPSHOT based on the
#   latest batch of positions (dt_insercao)
# - Ensure exposure consistency across OTC / Offshore / Swaps
#
# IMPORTANT:
# - Logic preserved exactly as original implementation
# ============================================================

# This function exists only to make partial re-runs idempotent.
def atualizar_exposi_risco_snapshot():
    # --------------------------------------------------------
    # 1. Get latest batch timestamp from positions table
    # --------------------------------------------------------
    max_dt = pd.read_sql(
        "SELECT MAX(dt_insercao) AS max_dt FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
        ENGINE
    ).iloc[0]["max_dt"]

    if max_dt is None:
        return

    max_dt_sql = max_dt.strftime("%Y-%m-%d %H:%M:%S")

    # --------------------------------------------------------
    # 2. Build snapshot dataset
    # --------------------------------------------------------
    query = f"""
        SELECT DISTINCT
            CAST(CgePortfolio AS SIGNED) AS cgePortfolio,
            NmClassificacao              AS origem,
            dt_insercao                  AS dt_carga
        FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS
        WHERE dt_insercao = '{max_dt_sql}'
    """
    df = pd.read_sql(query, ENGINE)

    if df.empty:
        return

    # --------------------------------------------------------
    # 3. Clean existing snapshot for the same batch
    # --------------------------------------------------------
    with ENGINE.begin() as conn:
        conn.execute(text("SET SQL_SAFE_UPDATES = 0;"))
        conn.execute(
            text("""
                DELETE FROM TB_ENQ_EXPOSI_RISCO_SNAPSHOT
                WHERE dt_carga = :dt
            """),
            {"dt": max_dt_sql}
        )
        conn.execute(text("SET SQL_SAFE_UPDATES = 1;"))

    # --------------------------------------------------------
    # 4. Persist snapshot
    # --------------------------------------------------------
    df.to_sql(
        name="TB_ENQ_EXPOSI_RISCO_SNAPSHOT",
        con=ENGINE,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )


# ============================================================
# JOB — SWAPS
# ============================================================
# Purpose:
# - Retrieve swap positions from Metabase
# - Align swaps with the same batch timestamp (dt_insercao)
# - Normalize layout to match positions table
# - Update exposure snapshot
#
# IMPORTANT:
# - Logic preserved exactly as original
# ============================================================

def run_swaps(log):
    log("Starting Swaps job...")

    # --------------------------------------------------------
    # Metabase private card (swaps)
    # --------------------------------------------------------
    url = f"{METABASE_BASE}/api/card/{METABASE_CARD_POSICOES_SWAP}/query/json"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    # --------------------------------------------------------
    # Define shared dt_insercao (same as positions batch)
    # --------------------------------------------------------
    query_dt = """
        SELECT MAX(dt_insercao) AS dt_insercao
        FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS;
    """
    df_dt = pd.read_sql(query_dt, ENGINE)

    if pd.notna(df_dt.iloc[0]["dt_insercao"]):
        dt_insercao_padrao = df_dt.iloc[0]["dt_insercao"]
    else:
        dt_insercao_padrao = pd.Timestamp.now()

    # --------------------------------------------------------
    # Identify latest available reference date
    # --------------------------------------------------------
    query_datas = """
        WITH BASE AS (
            SELECT
                h.CgePortfolio,
                DATE_FORMAT(h.data, '%%Y-%%m-01') AS mes_ref,
                MAX(h.data) AS ultima_data_mes
            FROM TB_ENQ_PL_HISTORICO h
            INNER JOIN (
                SELECT DISTINCT CgePortfolio
                FROM TB_ENQ_EXPOSI_RISCO_SNAPSHOT
            ) s ON s.CgePortfolio = h.CgePortfolio
            GROUP BY
                h.CgePortfolio,
                DATE_FORMAT(h.data, '%%Y-%%m-01')
        )
        SELECT
            CgePortfolio,
            mes_ref,
            ultima_data_mes
        FROM BASE
        ORDER BY
            CgePortfolio,
            mes_ref;
    """

    df_datas = pd.read_sql(query_datas, ENGINE)

    df_datas_unicas = (
        df_datas[["ultima_data_mes"]]
        .drop_duplicates()
        .sort_values("ultima_data_mes", ascending=False)
        .head(1)
    )

    # --------------------------------------------------------
    # API loop — retrieve swaps
    # --------------------------------------------------------
    dfs = []

    for row in df_datas_unicas.itertuples():
        data_carteira = pd.to_datetime(row.ultima_data_mes).strftime("%Y-%m-%d")

        body = (
            f'parameters=[{{'
            f'"type":"date/single",'
            f'"value":"{data_carteira}",'
            f'"target":["variable",["template-tag","DtCarteira"]],'
            f'"id":"8df24e4e-e7f5-d6ed-1585-bce1ad69569c"'
            f'}}]'
        )

        try:
            resp = mb_request(
                method="POST",
                url=url,
                headers=headers,
                data=body,
                timeout=REQUEST_TIMEOUT,
                verify=VERIFY_SSL
            )

            data = resp.json()
            if not data:
                continue

            df_tmp = pd.DataFrame(data)
            df_tmp["dt_carteira"] = data_carteira
            dfs.append(df_tmp)

        except Exception as e:
            log(f"WARNING — Swaps failed ({data_carteira}): {e}")

        sleep(REQUEST_RETRY_SLEEP)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if final_df.empty:
        log("No swaps returned.")
        return

    # --------------------------------------------------------
    # Normalize layout to positions schema
    # --------------------------------------------------------
    final_df["dt_carteira"] = pd.to_datetime(final_df["dt_carteira"], errors="coerce")

    final_df["Nickname"]        = final_df["DsIndiceAtivo"]
    final_df["notional"]        = final_df["ValorNocionalSwap"]
    final_df["NmClassificacao"] = final_df["origem"]
    final_df["NuIsin"]          = final_df["DsIndicePassivo"]
    final_df["DataCarteira"]    = final_df["dt_carteira"]

    final_df["ValorCotacao"]    = None
    final_df["qtyposicao"]      = None
    final_df["IdClassificacao"] = None
    final_df["valorfinanceiro"] = None
    final_df["CodAtivo"]        = None
    final_df["CodTipoAtivo"]    = None

    # Preserve batch timestamp
    final_df["dt_insercao"] = dt_insercao_padrao

    cols_final = [
        "Nickname", "DataCarteira", "notional", "CgePortfolio",
        "ValorCotacao", "NmClassificacao", "qtyposicao",
        "IdClassificacao", "valorfinanceiro", "CodAtivo",
        "NuIsin", "CodTipoAtivo", "dt_carteira", "dt_insercao",
        "ValorSaldoAtivoSwap", "ValorSaldoPassivoSwap"
    ]

    final_df = final_df[cols_final]

    # --------------------------------------------------------
    # Persist swaps
    # --------------------------------------------------------
    final_df.to_sql(
        name="TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
        con=ENGINE,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi"
    )

    # --------------------------------------------------------
    # Update risk exposure snapshot (same batch)
    # --------------------------------------------------------
    atualizar_exposi_risco_snapshot()

    log("Swaps job completed.")

# ============================================================
# JOB — FULL REMOTE → LOCAL DATABASE REPLICATION
# ============================================================
# Purpose:
# - Fully replicate ALL tables from the remote database
# - Preserve schema 1:1 (DDL, types, precision, nullability)
# - Copy all data into a local MySQL instance
#
# IMPORTANT:
# - This is NOT a dump/export
# - This is a full structural + data replication
# - Logic preserved exactly from original implementation
# ============================================================

def backup_local(log):
    log("Starting local database replication.")

    # ======================================================
    # 1. LIST ALL TABLES FROM REMOTE DATABASE
    # ======================================================

    def listar_tabelas():
        query = "SHOW TABLES;"
        df = pd.read_sql(query, ENGINE)
        tabelas = df.iloc[:, 0].tolist()

        print("\n=== TABLES FOUND IN REMOTE DATABASE ===")
        for t in tabelas:
            print(" -", t)

        return tabelas

    # ======================================================
    # 2. RECREATE LOCAL TABLE WITH IDENTICAL SCHEMA
    # ======================================================

    def recriar_tabela_local(tabela):
        print(f"\nRecreating table: {tabela}")

        # Retrieve remote DDL
        ddl_query = f"SHOW CREATE TABLE {tabela};"
        ddl_remote = pd.read_sql(ddl_query, ENGINE).iloc[0, 1]

        print(" - Remote DDL captured.")

        # Drop and recreate locally
        with ENGINE_LOCAL.begin() as conn:
            print(" - Dropping local table if exists...")
            conn.execute(text(f"DROP TABLE IF EXISTS {tabela};"))

            print(" - Creating local table...")
            conn.execute(text(ddl_remote))

        print(f"   Table {tabela} recreated locally.")

    # ======================================================
    # 3. COPY DATA FROM REMOTE TO LOCAL
    # ======================================================

    def copiar_dados(tabela):
        print(f"\nCopying data from table: {tabela}")

        df = pd.read_sql(f"SELECT * FROM {tabela};", ENGINE)
        print(f" - Rows found in remote: {len(df)}")

        if df.empty:
            print(" - Table empty. Nothing to copy.")
            return

        # Clean zero-date issues (only if columns exist)
        for col in ["dataProcessamento", "dataPosicao"]:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col].replace(
                        ["0000-00-00 00:00:00", "0000-00-00", ""],
                        None
                    ),
                    errors="coerce"
                )

        df.to_sql(
            tabela,
            con=ENGINE_LOCAL,
            if_exists="append",
            index=False,
            chunksize=2000,
            method="multi"
        )

        print(f"   Copied {len(df)} rows to local.")

    # ======================================================
    # 4. FULL REPLICATION PROCESS
    # ======================================================

    def replicar_completo():
        print("\n=======================================================")
        print("        STARTING REMOTE → LOCAL REPLICATION")
        print("=======================================================\n")

        tabelas = listar_tabelas()

        for tabela in tabelas:
            log(f"Processing table: {tabela}")

            print("\n-------------------------------------------------------")
            print(f"PROCESSING TABLE: {tabela}")
            print("-------------------------------------------------------")

            recriar_tabela_local(tabela)
            copiar_dados(tabela)

            log(f"Table {tabela} processed.")

        print("\n=======================================================")
        print("   REPLICATION FINISHED SUCCESSFULLY")
        print("=======================================================\n")

    # ======================================================
    # EXECUTION
    # ======================================================

    replicar_completo()
    log("Local database replication completed successfully.")
