SELECT p.*
FROM fact_positions_snapshot AS p
INNER JOIN (
    SELECT 
        portfolio_id,
        MAX(as_of_date) AS last_as_of_date
    FROM fact_positions_snapshot
    GROUP BY portfolio_id
) latest
    ON p.portfolio_id = latest.portfolio_id
   AND p.as_of_date = latest.last_as_of_date;
