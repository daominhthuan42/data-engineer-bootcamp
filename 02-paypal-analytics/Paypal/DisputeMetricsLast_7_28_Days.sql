USE PaypalAnalytic;
GO

WITH max_date AS
(
    SELECT  MAX(d.full_date) AS max_full_date
    FROM    gold.fact_disputePP02_dispute f
    JOIN    gold.dim_date d ON f.date_key = d.date_key
)
SELECT
    SUM(
        CASE
            WHEN d.full_date >= DATEADD(DAY,-7, m.max_full_date)
            THEN 1 ELSE 0
        END
    ) AS disputes_last_7_days,

    SUM(
        CASE
            WHEN d.full_date >= DATEADD(DAY,-28, m.max_full_date)
            THEN 1 ELSE 0
        END
    ) AS disputes_last_28_days

FROM        gold.fact_disputePP02_dispute f
JOIN        gold.dim_date d ON f.date_key = d.date_key
CROSS JOIN  max_date m;
GO
