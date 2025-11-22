SELECT *
FROM fact_manager_margin_snapshot
WHERE load_timestamp = (
    SELECT MAX(load_timestamp)
    FROM fact_manager_margin_snapshot
);
