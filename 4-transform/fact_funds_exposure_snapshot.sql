SELECT *
FROM fact_funds_exposure_snapshot
WHERE load_timestamp = (
    SELECT MAX(load_timestamp)
    FROM fact_funds_exposure_snapshot
);
