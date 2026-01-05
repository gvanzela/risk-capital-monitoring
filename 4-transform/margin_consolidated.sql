WITH 

/* -------------------- CARGAS ORIGINAIS -------------------- */

PL AS (
    SELECT 
        CAST(p.cgePortfolio AS SIGNED) AS cgePortfolio,
        p.nomeFundo,
        p.descClasseCvm,
        p.nomeGestor,
        p.publicoAlvo,
        CAST(p.pl AS DECIMAL(20,2)) AS pl,
        p.dt_carga AS dt_carga_pl
    FROM TB_ENQ_PL_SNAPSHOT p
    WHERE p.nomeGestor NOT LIKE '%BTG%'
      AND p.dt_carga = (SELECT MAX(dt_carga) FROM TB_ENQ_PL_SNAPSHOT)
),

EXPOSI AS (
    SELECT 
        CAST(e.CgePortfolio AS SIGNED) AS cgePortfolio,
        e.origem,
        e.dt_carga AS dt_carga_exposi
    FROM TB_ENQ_EXPOSI_RISCO_SNAPSHOT e
    WHERE e.dt_carga = (SELECT MAX(dt_carga) FROM TB_ENQ_EXPOSI_RISCO_SNAPSHOT)
),

ENTUBA AS (
    SELECT 
        CAST(m.CgePortfolio AS SIGNED) AS cgePortfolio,
        CAST(m.DataEnvio AS DATE) AS DataEnvio,
        CAST(m.MargemLocal AS DECIMAL(20,2)) AS MargemLocal,
        CAST(m.MargemOffshore AS DECIMAL(20,2)) AS MargemOffshore,
        m.dt_carga AS dt_carga_entuba
    FROM TB_ENQ_MARGEM_GESTOR_SNAPSHOT m
    WHERE m.dt_carga = (SELECT MAX(dt_carga) FROM TB_ENQ_MARGEM_GESTOR_SNAPSHOT)
),

V AS (
    SELECT 
        CAST(cge AS SIGNED) AS cge,
        MAX(CAST(data AS DATE)) AS data_validacao,
        MAX(status_valid) AS status_valid
    FROM TB_ENQ_VALIDACAO_MARGEM
    WHERE dt_carga = (SELECT MAX(dt_carga) FROM TB_ENQ_VALIDACAO_MARGEM)
    GROUP BY CAST(cge AS SIGNED)
),

E AS (
    SELECT CAST(cge AS SIGNED) AS cge, status
    FROM TB_ENQ_EXCECOES_MARGEM
),

BASE AS (
    SELECT
        PL.*,
        EXPOSI.origem,
        ENTUBA.DataEnvio,
        ENTUBA.MargemLocal,
        ENTUBA.MargemOffshore,
        EXPOSI.dt_carga_exposi,
        ENTUBA.dt_carga_entuba
    FROM PL
    LEFT JOIN EXPOSI ON PL.cgePortfolio = EXPOSI.cgePortfolio
    LEFT JOIN ENTUBA ON PL.cgePortfolio = ENTUBA.cgePortfolio
),

/* -------------------- DERIVADOS -------------------- */

DERIVADO AS (
    SELECT
        b.*,
        V.data_validacao,
        V.status_valid,
        COALESCE(E.status,0) AS status,

        CASE 
            WHEN b.DataEnvio IS NULL AND V.data_validacao IS NOT NULL THEN V.data_validacao
            ELSE b.DataEnvio
        END AS DataEnvioEfetiva,

        CASE 
            WHEN b.origem IN ('OTC OPC','OTC SWAP') THEN 'OTC'
            WHEN b.origem = 'Fundo Offshore' THEN 'OFFSHORE'
            ELSE 'N/A'
        END AS exposicao_consolidada

    FROM BASE b
    LEFT JOIN V ON b.cgePortfolio = V.cge
    LEFT JOIN E ON b.cgePortfolio = E.cge
),

/* -------------------- PERIODICIDADE -------------------- */

PER AS (
    SELECT
        *,
        CASE 
            WHEN exposicao_consolidada = 'OTC' AND publicoAlvo = 'Não qualificado' THEN 7
            WHEN exposicao_consolidada = 'OTC' AND publicoAlvo = 'Qualificado' THEN 30
            WHEN exposicao_consolidada = 'OTC' AND publicoAlvo = 'Investidor Profissional' THEN 90
            WHEN exposicao_consolidada = 'OFFSHORE' THEN 30
            ELSE NULL
        END AS periodicidade_dias
    FROM DERIVADO
),

/* -------------------- FLAG ENVIO POR VALIDAÇÃO -------------------- */

FINAL AS (
    SELECT
        *,
        CASE
            WHEN DataEnvio IS NULL
             AND data_validacao IS NOT NULL
             AND exposicao_consolidada IN ('OTC','OFFSHORE')
                THEN 1
            ELSE 0
        END AS flag_envio_por_validacao
    FROM PER
),

/* -------------------- ORDEM (ROW_NUMBER) -------------------- */

ORDEM AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            ORDER BY 
                CASE
                    WHEN flag_envio_por_validacao = 1 AND DataEnvioEfetiva IS NULL THEN 1   -- 100% exposição
                    WHEN DataEnvioEfetiva IS NULL THEN 2                                   -- sem envio
                    ELSE 3
                END,
                DataEnvioEfetiva,
                cgePortfolio
        ) AS UltimoEnvio_Ordem
    FROM FINAL f
)

SELECT
    cgePortfolio AS cge_fundo,
    nomeFundo AS fundo,
    descClasseCvm AS desc_classe_cvm,
    nomeGestor,
    publicoAlvo,
    pl,
    origem,

    DataEnvioEfetiva,
    DataEnvio,
    data_validacao,
    MargemLocal,
    MargemOffshore,
    periodicidade_dias,

    CASE 
        WHEN DataEnvioEfetiva IS NULL THEN NULL
        ELSE DATE_ADD(DataEnvioEfetiva, INTERVAL periodicidade_dias DAY)
    END AS data_limite_proximo_envio,

    CASE 
        WHEN DataEnvioEfetiva IS NULL THEN NULL
        ELSE DATEDIFF(
                DATE_ADD(DataEnvioEfetiva, INTERVAL periodicidade_dias DAY),
                CURDATE()
             )
    END AS dias_para_limite,

    CASE
        WHEN exposicao_consolidada = 'N/A' THEN 'N/A'
        WHEN DataEnvioEfetiva IS NULL THEN 'Sem Envio'
        WHEN CURDATE() <= DATE_ADD(DataEnvioEfetiva, INTERVAL periodicidade_dias DAY)
            THEN 'Em Dia'
        ELSE 'Pendente'
    END AS status_prazo,

    flag_envio_por_validacao,
    CASE WHEN DataEnvio IS NOT NULL THEN 1 ELSE 0 END AS flag_envio,
    CASE WHEN origem   IS NOT NULL THEN 1 ELSE 0 END AS flag_exposicao,

    COALESCE(MargemLocal,0) + COALESCE(MargemOffshore,0) AS margem_consolidada,

    exposicao_consolidada,
    status_valid,
    status,

    CASE 
        WHEN data_validacao IS NOT NULL THEN 'Recebido e Validado'
        WHEN status = 1 THEN 'Recebido e Validado'
        WHEN DataEnvio IS NOT NULL AND origem IS NOT NULL AND status_valid = 1 THEN 'Recebido e Validado'
        WHEN DataEnvio IS NOT NULL AND origem IS NOT NULL AND COALESCE(status_valid,0) = 0 THEN 'Recebido e N/ Validado'
        WHEN DataEnvio IS NULL AND origem IS NOT NULL THEN 'Não Recebido'
        ELSE 'N/A'
    END AS status_envio,

    CASE
        WHEN flag_envio_por_validacao = 1 AND DataEnvioEfetiva IS NULL
            THEN '100% da Exposição'
        ELSE DATE_FORMAT(DataEnvioEfetiva, '%d/%m/%Y')
    END AS UltimoEnvio_Ajustado,

    CASE
        WHEN flag_envio_por_validacao = 1 THEN 'N/A'
        ELSE 
            CASE
                WHEN exposicao_consolidada = 'N/A' THEN 'N/A'
                WHEN DataEnvioEfetiva IS NULL THEN 'Sem Envio'
                WHEN CURDATE() <= DATE_ADD(DataEnvioEfetiva, INTERVAL periodicidade_dias DAY)
                    THEN 'Em Dia'
                ELSE 'Pendente'
            END
    END AS StatusPrazo_Ajustado,

    UltimoEnvio_Ordem,

    dt_carga_pl,
    dt_carga_exposi,
    dt_carga_entuba

FROM ORDEM;
