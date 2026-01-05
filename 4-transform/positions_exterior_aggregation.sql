WITH BASE AS (
    SELECT 
        A.*,
        B.descricao,
        
        -- CLASSIFICAÇÃO (REGRA NOVA VS ANTIGA)
        CASE 
            WHEN A.dt_insercao >= '2025-12-02 15:45:56' THEN
                CASE
                    WHEN B.descricao = 'OPÇÃO BM&F'
                         OR A.NmClassificacao = 'OTC OPC'
                        THEN 'OPÇÃO BM&F'

                    WHEN A.NmClassificacao = 'OTC SWAP'
                        THEN 'OTC SWAP'

                    WHEN A.NmClassificacao = 'Investimento no Exterior'
                         OR (B.descricao IS NULL AND A.NmClassificacao = 'Investimento no Exterior')
                        THEN 'Exterior'

                    WHEN A.NmClassificacao = 'Fundo Offshore'
                        THEN 'Fundo Offshore'

                    ELSE 'Outros'
                END
        
            -- REGRA ANTIGA (antes da criação da classificação Fundo Offshore)
            ELSE
                CASE
                    WHEN B.descricao = 'OPÇÃO BM&F'
                         OR A.NmClassificacao = 'OTC OPC'
                        THEN 'OPÇÃO BM&F'

                    WHEN A.NmClassificacao = 'OTC SWAP'
                        THEN 'OTC SWAP'

                    -- OFFSHORE ANTIGO → vira Exterior para não perder histórico
                    WHEN A.NmClassificacao = 'Investimento no Exterior'
                         OR A.NmClassificacao = 'Fundo Offshore'
                         OR (B.descricao IS NULL AND A.NmClassificacao = 'Investimento no Exterior')
                        THEN 'Exterior'

                    ELSE 'Outros'
                END
        END AS ClassificacaoFiltro,

        ROW_NUMBER() OVER (
            PARTITION BY 
                A.CgePortfolio,
                A.Nickname,
                A.dt_carteira
            ORDER BY A.dt_insercao DESC
        ) AS rn

    FROM TB_ENQ_POSICOES_FUNDOS_EXPOSTOS A
    LEFT JOIN TB_ENQ_DE_PARA_COD_ATIVO B
           ON A.CodTipoAtivo = B.codTipoAtivo
),


-- APÓS DEDUPLICAÇÃO POR INSERÇÃO
FILTRADO AS (
    SELECT *
    FROM BASE
    WHERE rn = 1
),


-- AGRUPA SOMENTE OS VÁLIDOS (regra nova + antiga)
SOMA AS (
    SELECT
        CgePortfolio,
        DataCarteira,
        SUM(valorfinanceiro) AS SomaExterior_Offshore,
        dt_insercao,
        ClassificacaoFiltro
    FROM FILTRADO
    WHERE 
        (
            ClassificacaoFiltro = 'Exterior'
            AND dt_insercao < '2025-12-02 15:45:56'
        )
        OR
        (
            ClassificacaoFiltro = 'Fundo Offshore'
            AND dt_insercao >= '2025-12-02 15:45:56'
        )
    GROUP BY
        CgePortfolio,
        DataCarteira,
        dt_insercao,
        ClassificacaoFiltro
),


-- TRAZ APENAS A MAIOR DATACARTEIRA DO MÊS
FINAL AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CgePortfolio,
                YEAR(DataCarteira),
                MONTH(DataCarteira)
            ORDER BY DataCarteira DESC
        ) AS rn2
    FROM SOMA
)

SELECT
    CgePortfolio,
    DataCarteira,
    SomaExterior_Offshore
FROM FINAL
WHERE rn2 = 1
ORDER BY CgePortfolio, DataCarteira DESC
;
