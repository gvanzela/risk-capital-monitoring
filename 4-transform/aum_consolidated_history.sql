WITH portfolios AS (
    SELECT DISTINCT
        portfolio_id,
        fund_name,
        fund_class
    FROM fact_funds_aum_snapshot
    WHERE load_timestamp = (
        SELECT MAX(load_timestamp)
        FROM fact_funds_aum_snapshot
    )
),

aum_monthly AS (
    SELECT *
    FROM (
        SELECT
            h.*,
            ROW_NUMBER() OVER (
                PARTITION BY portfolio_id, YEAR(aum_date), MONTH(aum_date)
                ORDER BY aum_date DESC, load_timestamp DESC
            ) AS rn
        FROM fact_funds_aum_history h
    ) t
    WHERE rn = 1
),

margin_monthly AS (
    SELECT *
    FROM (
        SELECT
            portfolio_id,
            YEAR(sent_date) AS y,
            MONTH(sent_date) AS m,
            sent_date,
            ABS(COALESCE(local_margin,0)) + ABS(COALESCE(offshore_margin,0)) AS latest_margin,
            ROW_NUMBER() OVER (
                PARTITION BY portfolio_id, YEAR(sent_date), MONTH(sent_date)
                ORDER BY sent_date DESC
            ) AS rn
        FROM fact_manager_margin_snapshot
    ) mm
    WHERE rn = 1
)

SELECT
    p.fund_name,
    p.fund_class,
    a.*,
    m.latest_margin
FROM portfolios p
LEFT JOIN aum_monthly a
    ON p.portfolio_id = a.portfolio_id
LEFT JOIN margin_monthly m
    ON a.portfolio_id = m.portfolio_id
    AND YEAR(a.aum_date) = m.y
    AND MONTH(a.aum_date) = m.m;
