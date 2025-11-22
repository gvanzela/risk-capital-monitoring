WITH pl AS (
    SELECT
        s.portfolio_id,
        s.fund_name,
        s.fund_class,
        s.manager_name,
        s.investor_type,
        s.aum_value,
        s.load_timestamp AS load_pl
    FROM fact_funds_aum_snapshot AS s
    WHERE s.load_timestamp = (
        SELECT MAX(load_timestamp)
        FROM fact_funds_aum_snapshot
    )
),

exposure AS (
    SELECT
        e.portfolio_id,
        e.exposure_type,
        e.load_timestamp AS load_exposure
    FROM fact_funds_exposure_snapshot AS e
    WHERE e.load_timestamp = (
        SELECT MAX(load_timestamp)
        FROM fact_funds_exposure_snapshot
    )
),

margin AS (
    SELECT
        m.portfolio_id,
        m.sent_date,
        m.local_margin,
        m.offshore_margin,
        m.load_timestamp AS load_margin
    FROM fact_manager_margin_snapshot AS m
    WHERE m.load_timestamp = (
        SELECT MAX(load_timestamp)
        FROM fact_manager_margin_snapshot
    )
),

validation AS (
    SELECT
        v.portfolio_id,
        MAX(v.validation_date) AS last_validation_date,
        MAX(v.validation_status) AS validation_status
    FROM fact_margin_validation AS v
    WHERE v.load_timestamp = (
        SELECT MAX(load_timestamp)
        FROM fact_margin_validation
    )
    GROUP BY v.portfolio_id
),

exceptions AS (
    SELECT
        e.portfolio_id,
        e.exception_flag
    FROM fact_margin_e
