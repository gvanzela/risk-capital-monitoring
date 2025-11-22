WITH posi AS (
    SELECT
        p.*,
        m.asset_label AS asset_mapping_label,

        CASE
            WHEN m.asset_label = 'OPTION_EXCHANGE'
                THEN 'OPTION_EXCHANGE'
            WHEN m.asset_label IS NULL
                 AND p.asset_class = 'FOREIGN_INVESTMENT'
                THEN 'EXTERIOR'
            ELSE 'OTHER'
        END AS classification_filter,

        ROW_NUMBER() OVER (
            PARTITION BY
                p.portfolio_id,
                p.asset_nickname,
                YEAR(p.as_of_date),
                MONTH(p.as_of_date)
            ORDER BY p.as_of_date DESC
        ) AS rn

    FROM fact_positions_snapshot AS p
    LEFT JOIN dim_asset_mapping AS m
           ON p.asset_type_id = m.asset_type_id
)

SELECT
    portfolio_id,
    as_of_date,
    SUM(position_value) AS total_exterior_value
FROM posi
WHERE rn = 1
  AND classification_filter = 'EXTERIOR'
GROUP BY
    portfolio_id,
    as_of_date;
