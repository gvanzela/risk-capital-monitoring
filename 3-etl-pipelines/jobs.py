# ============================================================
# IMPORTS GLOBAIS
# ============================================================

import requests
import pandas as pd
import re
from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta
from time import sleep


# ============================================================
# BANCO (ENGINE ÚNICO GLOBAL)
# ============================================================

ENGINE = create_engine(
    "",
    pool_pre_ping=True
)

# ENGINE_LOCAL = create_engine(
#     "mysql+pymysql://root:G%40briel11092025@10.11.0.106:3306/liquids_data"
# )

ENGINE_LOCAL = create_engine(
    "mysql+pymysql://root:G%40briel11092025@10.20.12.118:3306/liquids_data"
) #JOSE BONIFÁCIO


# ============================================================
# METABASE – LOGIN AUTOMÁTICO
# ============================================================

METABASE_BASE = ""
METABASE_USER = ""
METABASE_PASS = ""

# Session mantém cookie automaticamente
SESSION = requests.Session()


def metabase_login():
    """
    Faz login no Metabase e grava o cookie de sessão no SESSION.
    """
    r = SESSION.post(
        f"{METABASE_BASE}/api/session",
        json={
            "username": METABASE_USER,
            "password": METABASE_PASS
        },
        timeout=30
    )
    r.raise_for_status()


def mb_request(method, url, **kwargs):
    """
    Wrapper para substituir requests.get / requests.post quando for Metabase.

    Fluxo:
    - tenta a request
    - se vier 401 → refaz login
    - repete a request
    - se falhar, levanta erro
    """
    r = SESSION.request(method, url, **kwargs)

    if r.status_code == 401:
        metabase_login()
        r = SESSION.request(method, url, **kwargs)

    r.raise_for_status()
    return r


# Cada função abaixo será chamada pelo app
# Substitua o comentário "SEU CÓDIGO AQUI" pelo seu código real.

def run_margem(log):
    log("Iniciando Margem Gestor...")

    url = "***" #type url
    params = {"parameters": ""}

    # === Requisição protegida ===
    try:
        resp = mb_request("GET", url, params=params, timeout=300)
        data = resp.json()
    except Exception as e:
        log(f"ERRO Margem Gestor: {e}")
        return  # <<< libera o fluxo (Rodar TUDO continua)

    # === DataFrame ===
    df = pd.DataFrame(data)

    if df.empty:
        log("Margem Gestor sem dados retornados.")
        return

    # === Ajustes de tipo ===
    if "DataEnvio" in df.columns:
        df["DataEnvio"] = pd.to_datetime(df["DataEnvio"], errors="coerce")

    if "MargemLocal" in df.columns:
        df["MargemLocal"] = pd.to_numeric(df["MargemLocal"], errors="coerce")

    if "MargemOffshore" in df.columns:
        df["MargemOffshore"] = pd.to_numeric(df["MargemOffshore"], errors="coerce")

    # === dt_carga BRT ===
    tz_brt = timezone(timedelta(hours=-3))
    df["dt_carga"] = datetime.now(tz_brt)

    # === Insert ===
    df.to_sql(
        "TB_ENQ_MARGEM_GESTOR_SNAPSHOT",
        con=ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    log(f"Margem Gestor inserido: {len(df)} linhas")




def run_pl_snapshot(log):
    log("Iniciando PL Snapshot...")

    import requests
    import pandas as pd
    import re
    from sqlalchemy import create_engine
    from datetime import datetime

    url = "http://fundsplatform/painel-risco/api/monitor-risco/get-risco-nivel1"

    headers = {
        "Cookie": "", #type the cookie 
        "x-crypto-token": "" #type de session token
    }

    # conexão com o banco
    engine = create_engine("mysql+pymysql://L_PROTO-XML_CAPEF:XAByRkt5ClcxQPe@CTB-DEV1:3306/PROTO-XML_CAPEF")

    # faz a requisição
    resp = requests.get(url, headers=headers, verify=False)  # verify=False se for https interno
    data = resp.json()

    # transforma em DataFrame base
    df = pd.DataFrame(data)

    # expande a coluna 'resultado'
    df_expandido = pd.json_normalize(df['resultado'], sep='_')

    # remove o prefixo 'detalhesJson.' ou 'detalhesJson_'
    df_expandido.columns = [re.sub(r'^(detalhesJson[._])', '', c) for c in df_expandido.columns]

    # junta com as colunas originais (mantendo sucesso/erros)
    df_final = pd.concat([df.drop(columns=['resultado']), df_expandido], axis=1)

    # adiciona dt_carga
    df_final["dt_carga"] = datetime.now()

    # mantém só colunas que existem no banco
    colunas_validas = [
        "sucesso", "erros", "cgePortfolio", "dataProcessamento", "dataPosicao",
        "nomeFundo", "cnpj", "cgeGestor", "nomeGestor", "publicoAlvo",
        "tipoPortfolio", "classAnbidScp", "fundoFechado", "restricaoInvestimento",
        "classeCvm", "descClasseCvm", "pl", "margemPl", "contaCorrente",
        "saldoCc", "derivativos", "derivativosRisco", "derivativosPl",
        "derivativosRiscoPl", "liquidezPl", "rentabilidadeDia", "rentabilidadeMes",
        "rentabilidadeAno", "criticidade", "var95", "var99", "bull", "bear",
        "bullParis", "bearParis", "var95Paris3M", "var95Paris1Y", "var95Paris2Y",
        "var99Paris3M", "var99Paris1Y", "var99Paris2Y", "fiiFiq", "dt_carga"
    ]

    df_final = df_final[colunas_validas]

    # grava no banco em append
    df_final.to_sql(
        "TB_ENQ_PL_SNAPSHOT",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Snapshot inserido com sucesso!")
    
    engine.dispose()

    log("PL Snapshot finalizado.")

def run_pl_historico(log, data_carteira):
    log(f"Iniciando PL Histórico ({data_carteira})...")

    # === URL Metabase (card público) ===
    url = "" #type url

    params = {
        "parameters": (
            f'[{{"type":"date/single","value":"{data_carteira}",'
            f'"target":["variable",["template-tag","DataCarteira"]]}}]'
        )
    }

    # === Requisição com login automático ===
    resp = mb_request("GET", url, params=params, timeout=120)
    data = resp.json()

    # === DataFrame ===
    df = pd.DataFrame(data)

    # === Ajustes ===
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

    # === Insert ===
    df.to_sql(
        "TB_ENQ_PL_HISTORICO",
        con=ENGINE,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    print(f"Foto diária ({data_carteira}) inserida com sucesso! Total de linhas: {len(df)}")
    log("PL Histórico finalizado.")


def run_posicoes(log):
    """
    Coleta posições dos fundos (OTC, Offshore e Swap),
    normaliza o layout e insere em TB_ENQ_POSICOES_FUNDOS_EXPOSTOS.
    Usa sessão autenticada do Metabase (mb_request) e ENGINE global.
    """
    log("Iniciando Posições de Fundos...")

    from time import sleep

    # ============================================================
    # CONFIGURAÇÕES DE ENDPOINT
    # ============================================================

    # Cards privados (exigem sessão autenticada)
    METABASE_URL = "" #url

    # Card público (não exige cookie manual, mas usa mesma sessão)
    PUBLIC_URL = (
        "" #type url
        "" #type url complement
    )

    HEADERS = {
        "Content-Type": ""
    }

    # ============================================================
    # 1. DATA GLOBAL MAIS RECENTE (BASE DE PL)
    # ============================================================

    def get_maior_data():
        """
        Busca a maior data disponível na TB_ENQ_PL_HISTORICO.
        Usada como referência global para puxar CGEs expostos.
        """
        query = """
            SELECT MAX(data) AS ultima_data
            FROM TB_ENQ_PL_HISTORICO;
        """
        df = pd.read_sql(query, ENGINE)
        return df.iloc[0]["ultima_data"].strftime("%Y-%m-%d")

    # ============================================================
    # 2. CONSULTA CARD PRIVADO DO METABASE
    # ============================================================

    def consultar_card(card_id, data_ref):
        """
        Consulta card do Metabase.
        Em erro (timeout / 5xx / 401), retorna DF vazio e NÃO trava o fluxo.
        """
        url = METABASE_URL.format(card_id=card_id)

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
                timeout=300  # ↑ maior p/ offshore
            )
            data = resp.json()
            return pd.DataFrame(data) if data else pd.DataFrame()

        except Exception as e:
            print(f"ERRO card {card_id}: {e}")
            return pd.DataFrame(columns=["CgePortfolio"])

    # ============================================================
    # 3. FUNÇÕES AUXILIARES (CGEs / POSIÇÕES)
    # ============================================================

    def get_cges(card_id, data_ref):
        """
        Retorna lista única de CGEs expostos para um card.
        """
        df = consultar_card(card_id, data_ref)
        if df.empty:
            return pd.DataFrame()#(columns=["CgePortfolio"])
        return df

    def get_posicoes_cges(card_id, data_ref):
        """
        Retorna posições completas (usado depois para
        classificar OTC x Offshore).
        """
        df = consultar_card(card_id, data_ref)
        return df.drop_duplicates() if not df.empty else pd.DataFrame()

    # ============================================================
    # 4. EXECUÇÃO — PARTE 1 (LISTA DE CGEs)
    # ============================================================

    data_ref_global = get_maior_data()
    # data_ref_global = "2025-11-26"

    df_otc_full  = get_cges(2721, data_ref_global)
    df_swap_full = get_cges(2693, data_ref_global)
    df_off_full  = get_cges(2722, data_ref_global)


    df_otc = df_otc_full[["CgePortfolio"]].drop_duplicates()
    df_swap = df_swap_full[["CgePortfolio"]].drop_duplicates()
    df_off = df_off_full[["CgePortfolio"]].drop_duplicates()

    # trava de integridade – só continua se TODOS retornarem dados
    if df_otc.empty or df_swap.empty or df_off.empty:
        log("Abortando: um ou mais cards não retornaram dados (OTC / SWAP / OFF).")
        return


    df_cges = (
        pd.concat([df_otc, df_off, df_swap], ignore_index=True)
          .drop_duplicates()
    )

    print(f"CGEs expostos identificados: {len(df_cges)}")

    # ============================================================
    # 5. BUSCAR ÚLTIMA DATA INDIVIDUAL POR CGE
    # ============================================================

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

    # ============================================================
    # 6. EXECUÇÃO — PARTE 2 (POSIÇÕES POR CGE)
    # ============================================================

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
                f'"type":"number/=",'
                f'"value":["{cge}"],'
                f'"target":["variable",["template-tag","CGE"]]'
                f'}}]'
            )
        }

        try:
            resp = mb_request(
                method="GET",
                url=PUBLIC_URL,
                params=params,
                timeout=120
            )

            data = resp.json()
            if not data:
                continue

            df_temp = pd.DataFrame(data)
            df_temp["CgePortfolio"] = cge
            df_temp["dt_carteira"] = data_carteira

            dfs.append(df_temp)

        except Exception as e:
            print(f"Erro ao consultar CGE {cge}: {e}")

        sleep(0.4)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print(f"Total de linhas coletadas: {len(final_df)}")

    # ============================================================
    # 7. TRATAMENTOS E CONVERSÕES
    # ============================================================

    if not final_df.empty:

        final_df["DataCarteira"] = pd.to_datetime(
            final_df["DataCarteira"], errors="coerce"
        )
        final_df["dt_carteira"] = pd.to_datetime(
            final_df["dt_carteira"], errors="coerce"
        )

        colunas_numericas = [
            "notional",
            "ValorCotacao",
            "qtyposicao",
            "IdClassificacao",
            "valorfinanceiro"
        ]

        for col in colunas_numericas:
            if col in final_df.columns:
                final_df[col] = (
                    pd.to_numeric(final_df[col], errors="coerce")
                      .round(6)
                )

        colunas_int = [
            "CodAtivo",
            "CodTipoAtivo",
            "CgePortfolio"
        ]

        for col in colunas_int:
            if col in final_df.columns:
                final_df[col] = (
                    pd.to_numeric(final_df[col], errors="coerce")
                      .astype("Int64")
                )

        colunas_str = [
            "Nickname",
            "NmClassificacao",
            "NuIsin"
        ]

        for col in colunas_str:
            if col in final_df.columns:
                final_df[col] = final_df[col].astype(str).str.strip()

    # ============================================================
    # 8. PADRONIZAÇÃO DO LAYOUT FINAL
    # ============================================================

    cols_final = [
        "Nickname",
        "DataCarteira",
        "notional",
        "CgePortfolio",
        "ValorCotacao",
        "NmClassificacao",
        "qtyposicao",
        "IdClassificacao",
        "valorfinanceiro",
        "CodAtivo",
        "NuIsin",
        "CodTipoAtivo",
        "dt_carteira",
        "dt_insercao",
        "ValorSaldoAtivoSwap",
        "ValorSaldoPassivoSwap"
    ]

    # Garante que todas as colunas existam
    for col in cols_final:
        if col not in final_df.columns:
            final_df[col] = None

    final_df["dt_insercao"] = pd.Timestamp.now()
    final_df = final_df[cols_final]

    # ============================================================
    # 9. AJUSTE FINAL DE CLASSIFICAÇÃO
    # ============================================================

    nicks_off = set(df_off_full["Nickname"])
    nicks_otc = set(df_otc_full["Nickname"])


    final_df.loc[
        final_df["Nickname"].isin(nicks_off),
        "NmClassificacao"
    ] = "Fundo Offshore"

    final_df.loc[
        final_df["Nickname"].isin(nicks_otc),
        "NmClassificacao"
    ] = "OTC OPC"

    # ============================================================
    # 10. INSERT NO BANCO
    # ============================================================

    if not final_df.empty:
        final_df.to_sql(
            name="TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
            con=ENGINE,
            if_exists="append",
            index=False,
            chunksize=2000,
            method="multi"
        )

    log("Posições finalizado.")


# Atualizar base de fundos expostos
def atualizar_exposi_risco_snapshot():
    # 1) maior dt_insercao
    max_dt = pd.read_sql(
        "SELECT MAX(dt_insercao) AS max_dt FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
        ENGINE
    ).iloc[0]["max_dt"]

    if max_dt is None:
        return

    max_dt_sql = max_dt.strftime("%Y-%m-%d %H:%M:%S")

    # 2) dados do snapshot
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

    # 3) limpar snapshot existente (safe updates off/on)
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

    # 4) insert
    df.to_sql(
        "TB_ENQ_EXPOSI_RISCO_SNAPSHOT",
        con=ENGINE,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )


def run_swaps(log):
    """
    Coleta swaps do Metabase, normaliza layout e insere em
    TB_ENQ_POSICOES_FUNDOS_EXPOSTOS usando a MESMA dt_insercao
    das posições já carregadas.
    """
    log("Iniciando Swaps...")

    from time import sleep

    # ============================================================
    # CONFIGURAÇÕES
    # ============================================================

    url = "" #type url

    headers = {
        "Content-Type": "" #type content type
    }

    # ============================================================
    # 0. DEFINIR dt_insercao ÚNICA (MESMA DAS POSIÇÕES)
    # ============================================================

    query_dt = """
        SELECT MAX(dt_insercao) AS dt_insercao
        FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS;
    """
    df_dt = pd.read_sql(query_dt, ENGINE)

    if pd.notna(df_dt.iloc[0]["dt_insercao"]):
        dt_insercao_padrao = df_dt.iloc[0]["dt_insercao"]
    else:
        dt_insercao_padrao = pd.Timestamp.now()

    # ============================================================
    # 1. BUSCAR ÚLTIMA DATA DISPONÍVEL POR FUNDO
    # ============================================================

    query = """
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

    df_datas = pd.read_sql(query, ENGINE)

    df_datas_unicas = (
        df_datas[["ultima_data_mes"]]
        .drop_duplicates()
        .sort_values("ultima_data_mes", ascending=False)
        .head(1)
    )

    # ============================================================
    # 2. LOOP API (SWAPS)
    # ============================================================

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
                timeout=120
            )

            data = resp.json()
            if not data:
                continue

            df_temp = pd.DataFrame(data)
            df_temp["dt_carteira"] = data_carteira
            dfs.append(df_temp)

        except Exception as e:
            print(f"Erro swaps ({data_carteira}): {e}")

        sleep(0.5)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if final_df.empty:
        log("Nenhum swap retornado.")
        return

    # ============================================================
    # 3. CONVERSÕES
    # ============================================================

    final_df["dt_carteira"] = pd.to_datetime(final_df["dt_carteira"], errors="coerce")

    # ============================================================
    # 4. PADRONIZAÇÃO PARA TABELA DE POSIÇÕES
    # ============================================================

    final_df["Nickname"]        = final_df["DsIndiceAtivo"]
    final_df["notional"]        = final_df["ValorNocionalSwap"]
    final_df["NmClassificacao"] = final_df["origem"]
    final_df["NuIsin"]          = final_df["DsIndicePassivo"]

    final_df["DataCarteira"] = final_df["dt_carteira"]

    final_df["ValorCotacao"]    = None
    final_df["qtyposicao"]      = None
    final_df["IdClassificacao"] = None
    final_df["valorfinanceiro"] = None
    final_df["CodAtivo"]        = None
    final_df["CodTipoAtivo"]    = None

    # date filter
    final_df["dt_insercao"] = dt_insercao_padrao

    # ============================================================
    # 5. ORDENAR COLUNAS
    # ============================================================

    cols_final = [
        "Nickname",
        "DataCarteira",
        "notional",
        "CgePortfolio",
        "ValorCotacao",
        "NmClassificacao",
        "qtyposicao",
        "IdClassificacao",
        "valorfinanceiro",
        "CodAtivo",
        "NuIsin",
        "CodTipoAtivo",
        "dt_carteira",
        "dt_insercao",
        "ValorSaldoAtivoSwap",
        "ValorSaldoPassivoSwap"
    ]

    final_df = final_df[cols_final]

    # ============================================================
    # 6. INSERT
    # ============================================================

    final_df.to_sql(
        name="TB_ENQ_POSICOES_FUNDOS_EXPOSTOS",
        con=ENGINE,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi"
    )

    # Atualizar fundos expostos
    atualizar_exposi_risco_snapshot()

    log("Swaps finalizado.")



def backup_local(log):
    log("Iniciando Backup.")
    """
    Replica TODAS as tabelas do banco remoto para o banco local,
    preservando 100% do schema (tipos, precision, null, etc).
    """
    from sqlalchemy import create_engine, text

    # ======================================================
    # 1. LISTAR TABELAS DO REMOTO
    # ======================================================

    def listar_tabelas():
        query = "SHOW TABLES;"
        df = pd.read_sql(query, ENGINE)
        tabelas = df.iloc[:, 0].tolist()
        print("\n=== TABELAS ENCONTRADAS NO REMOTO ===")
        for t in tabelas:
            print(" -", t)
        return tabelas

    # ======================================================
    # 2. RECRIA TABELA LOCAL IDENTICA AO REMOTO
    # ======================================================

    def recriar_tabela_local(tabela):
        print(f"\n🔧 RECRIANDO TABELA: {tabela}")

        # pegar DDL remoto
        ddl_query = f"SHOW CREATE TABLE {tabela};"
        ddl_remote = pd.read_sql(ddl_query, ENGINE).iloc[0, 1]

        print(" - DDL remoto capturado.")

        # dropar e recriar no local
        with ENGINE_LOCAL.begin() as conn:
            print(" - Dropando tabela local (se existir)...")
            conn.execute(text(f"DROP TABLE IF EXISTS {tabela};"))

            print(" - Criando tabela no local...")
            conn.execute(text(ddl_remote))

        print(f"   ✔ Tabela {tabela} recriada no local.")

    # ======================================================
    # 3. COPIAR DADOS DO REMOTO PARA O LOCAL
    # ======================================================

    def copiar_dados(tabela):
        print(f"\n COPIANDO DADOS: {tabela}")

        df = pd.read_sql(f"SELECT * FROM {tabela};", ENGINE)
        print(f" - Linhas encontradas no remoto: {len(df)}")

        if df.empty:
            print(" - Nada a copiar (tabela vazia).")
            return

        # limpar zero-date SOMENTE se as colunas existirem
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

        print(f"   Copiado para local: {len(df)} linhas.")

    # ======================================================
    # 4. PROCESSO COMPLETO (ESTRUTURA + DADOS)
    # ======================================================

    def replicar_completo():
        print("\n=======================================================")
        print("      INICIANDO REPLICAÇÃO REMOTO → LOCAL")
        print("=======================================================\n")

        tabelas = listar_tabelas()

        for tabela in tabelas:
            log(f"Processando tabela: {tabela}")
            print("\n-------------------------------------------------------")
            print(f"PROCESSANDO TABELA: {tabela}")
            print("-------------------------------------------------------")

            recriar_tabela_local(tabela)
            copiar_dados(tabela)
            log(f"Tabela {tabela} processada.")

        print("\n=======================F================================")
        print("   REPLICAÇÃO FINALIZADA COM SUCESSO")
        print("=======================================================\n")


    # ======================================================
    # EXECUTAR
    # ======================================================

    replicar_completo()

    log("Backup local finalizado com sucesso.")

