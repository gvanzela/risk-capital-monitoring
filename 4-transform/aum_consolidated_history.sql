WITH dados_cge AS (
    SELECT DISTINCT
        cgePortfolio,
        nomeFundo,
        descClasseCvm
    FROM TB_ENQ_PL_SNAPSHOT
    WHERE dt_carga = (SELECT MAX(dt_carga) FROM TB_ENQ_PL_SNAPSHOT)
    ORDER BY cgePortfolio
),
dados_pl AS (
    SELECT *
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY cgePortfolio, YEAR(`data`), MONTH(`data`)
                ORDER BY `data` DESC, `dt_carga` DESC
            ) AS rn
        FROM TB_ENQ_PL_HISTORICO t
    ) sub
    WHERE rn = 1
),
margem_mes AS (
SELECT *
FROM (
    SELECT 
        CgePortfolio,
        YEAR(DataEnvio)  AS AnoEnvio,
        MONTH(DataEnvio) AS MesEnvio,
        DataEnvio,
        ABS(COALESCE(MargemLocal,0)) + ABS(COALESCE(MargemOffshore,0)) AS MargemMaisRecente,
        ROW_NUMBER() OVER (
            PARTITION BY CgePortfolio, YEAR(DataEnvio), MONTH(DataEnvio)
            ORDER BY DataEnvio DESC
        ) AS rn
    FROM TB_ENQ_MARGEM_GESTOR_SNAPSHOT
) A
WHERE rn = 1
),
dados_final AS (
    SELECT
        a.nomeFundo,
        a.descClasseCvm,
        b.*,
        m.MargemMaisRecente
    FROM dados_cge a
    LEFT JOIN dados_pl b
        ON a.cgePortfolio = b.cgePortfolio
    LEFT JOIN margem_mes m
        ON b.cgePortfolio = m.CgePortfolio
        AND YEAR(b.`data`) = m.AnoEnvio
        AND MONTH(b.`data`) = m.MesEnvio
)
SELECT * FROM dados_final;
