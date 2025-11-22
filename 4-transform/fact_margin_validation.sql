SELECT *
FROM fact_margin_validation
WHERE load_timestamp = (
    SELECT MAX(load_timestamp)
    FROM fact_margin_validation
);
