WITH base AS (
    SELECT 
        t.*
    FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS t
    INNER JOIN (
        SELECT 
            CgePortfolio,
            MAX(dt_carteira) AS Maxdt_carteira
        FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS
        GROUP BY CgePortfolio
    ) ult
        ON t.CgePortfolio = ult.CgePortfolio
       AND t.dt_carteira = ult.Maxdt_carteira
),

ordenado AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CgePortfolio,
                dt_carteira,
                Nickname,
                CASE 
                    WHEN nMCLASSIFICACAO = 'OTC SWAP' 
                        THEN CONCAT(notional, '_', ValorSaldoAtivoSwap, '_', ValorSaldoPassivoSwap)
                    ELSE ''
                END
            ORDER BY dt_insercao DESC
        ) AS rn
    FROM base
)

SELECT *
FROM ordenado
WHERE rn = 1;
