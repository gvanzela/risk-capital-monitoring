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
    FROM fact_margin_exceptions AS e
)

,

base AS (
    SELECT
        pl.portfolio_id,
        pl.fund_name,
        pl.fund_class,
        pl.manager_name,
        pl.investor_type,
        pl.aum_value,

        exposure.exposure_type,
        margin.sent_date,
        margin.local_margin,
        margin.offshore_margin,

        pl.load_pl,
        exposure.load_exposure,
        margin.load_margin

    FROM pl
    LEFT JOIN exposure ON pl.portfolio_id = exposure.portfolio_id
    LEFT JOIN margin   ON pl.portfolio_id = margin.portfolio_id
)

SELECT
    base.portfolio_id,
    base.fund_name,
    base.fund_class,
    base.manager_name,
    base.investor_type,
    base.aum_value,
    base.exposure_type,

    /* DATA EFETIVA */
    CASE
        WHEN base.sent_date IS NULL AND validation.last_validation_date IS NOT NULL
            THEN validation.last_validation_date
        ELSE base.sent_date
    END AS effective_sent_date,

    base.sent_date,
    validation.last_validation_date,

    base.local_margin,
    base.offshore_margin,

    /* PERIODICITY RULE */
    CASE
        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'RETAIL'  THEN 7
        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'QUALIFIED' THEN 30
        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'PROFESSIONAL' THEN 90
        WHEN base.exposure_type = 'OFFSHORE' THEN 30
        ELSE NULL
    END AS periodicity_days,

    /* DUE DATE CALC */
    CASE
        WHEN effective_sent_date IS NULL THEN NULL
        ELSE DATE_ADD(
            effective_sent_date,
            INTERVAL (
                CASE
                    WHEN base.exposure_type = 'OTC' AND base.investor_type = 'RETAIL' THEN 7
                    WHEN base.exposure_type = 'OTC' AND base.investor_type = 'QUALIFIED' THEN 30
                    WHEN base.exposure_type = 'OTC' AND base.investor_type = 'PROFESSIONAL' THEN 90
                    WHEN base.exposure_type = 'OFFSHORE' THEN 30
                END
            ) DAY
        )
    END AS next_due_date,

    /* DAYS TO DUE */
    CASE
        WHEN effective_sent_date IS NULL THEN NULL
        ELSE DATEDIFF(
            DATE_ADD(
                effective_sent_date,
                INTERVAL (
                    CASE
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'RETAIL' THEN 7
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'QUALIFIED' THEN 30
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'PROFESSIONAL' THEN 90
                        WHEN base.exposure_type = 'OFFSHORE' THEN 30
                    END
                ) DAY
            ),
            CURDATE()
        )
    END AS days_to_due,

    /* DEADLINE STATUS */
    CASE
        WHEN base.exposure_type NOT IN ('OTC', 'OFFSHORE') THEN 'N/A'
        WHEN effective_sent_date IS NULL THEN 'NO_SENT'
        WHEN CURDATE() <= DATE_ADD(
                effective_sent_date,
                INTERVAL (
                    CASE
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'RETAIL' THEN 7
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'QUALIFIED' THEN 30
                        WHEN base.exposure_type = 'OTC' AND base.investor_type = 'PROFESSIONAL' THEN 90
                        WHEN base.exposure_type = 'OFFSHORE' THEN 30
                    END
                ) DAY
            )
            THEN 'ON_TIME'
        ELSE 'LATE'
    END AS deadline_status,

    /* VALIDATION FLAG */
    CASE
        WHEN base.sent_date IS NULL
         AND validation.last_validation_date IS NOT NULL
         AND base.exposure_type IN ('OTC','OFFSHORE')
            THEN 1
        ELSE 0
    END AS flag_sent_by_validation,

    /* OTHER FLAGS */
    CASE WHEN base.sent_date IS NOT NULL THEN 1 ELSE 0 END AS flag_sent,
    CASE WHEN base.exposure_type IS NOT NULL THEN 1 ELSE 0 END AS flag_exposed,

    /* CONSOLIDATED MARGIN */
    COALESCE(base.local_margin,0) + COALESCE(base.offshore_margin,0) AS consolidated_margin,

    /* CONSOLIDATED EXPOSURE */
    CASE
        WHEN base.exposure_type = 'OTC' THEN 'OTC'
        WHEN base.exposure_type = 'OFFSHORE' THEN 'OFFSHORE'
        ELSE 'NONE'
    END AS consolidated_exposure,

    validation.validation_status,
    exceptions.exception_flag,

    /* FINAL STATUS */
    CASE
        WHEN validation.last_validation_date IS NOT NULL THEN 'VALIDATED'
        WHEN exceptions.exception_flag = 1 THEN 'VALIDATED'
        WHEN base.sent_date IS NOT NULL AND base.exposure_type IS NOT NULL AND validation.validation_status = 1 THEN 'VALIDATED'
        WHEN base.sent_date IS NOT NULL AND base.exposure_type IS NOT NULL AND validation.validation_status = 0 THEN 'SENT_NOT_VALIDATED'
        WHEN base.sent_date IS NULL AND base.exposure_type IS NOT NULL THEN 'NOT_SENT'
        ELSE 'N/A'
    END AS status_final,

    base.load_pl,
    base.load_exposure,
    base.load_margin

FROM base
LEFT JOIN validation ON base.portfolio_id = validation.portfolio_id
LEFT JOIN exceptions ON base.portfolio_id = exceptions.portfolio_id;
