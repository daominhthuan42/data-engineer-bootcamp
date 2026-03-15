/*
Purpose:
Populate the Gold dimension table gold.dim_date with distinct calendar
dates derived from transactional and dispute event data in the Silver layer.

Processing logic:
1. Remove all existing records from gold.dim_date using TRUNCATE TABLE
   to support a full reload of the date dimension.
2. Collect all relevant event dates from the Silver layer:
   - transaction_initiation_date from silver.disputed_pp01_transactions
   - create_time from silver.disputed_pp02_disputes
3. Normalize these datetime values by casting them to DATE to ensure
   consistent day-level granularity.
4. Combine the date sources using UNION to produce a unified list of
   distinct event dates.
5. Generate a surrogate date_key in YYYYMMDD format using FORMAT and
   CONVERT for compatibility with star schema modeling.
6. Derive standard calendar attributes including year, quarter, month,
   day, day_of_week, and month_name using built-in SQL date functions.
7. Exclude NULL dates to maintain valid dimension records.
8. Prevent duplicate date inserts by verifying that the full_date does
   not already exist in the target dimension table using NOT EXISTS.
9. Insert the resulting calendar records into the Gold date dimension.

This step supports the dimensional model within the Medallion architecture
by providing a reusable date dimension used by fact tables for time-based
analysis, reporting, and aggregation across transaction and dispute data.
*/

TRUNCATE TABLE gold.dim_date;
GO
INSERT INTO gold.dim_date
(
    date_key,
    full_date,
    year,
    quarter,
    month,
    day,
    day_of_week,
    month_name
)
SELECT DISTINCT
    CONVERT(INT, FORMAT(event_date,'yyyyMMdd')),
    event_date,
    YEAR(event_date),
    DATEPART(QUARTER,event_date),
    MONTH(event_date),
    DAY(event_date),
    DATEPART(WEEKDAY,event_date),
    DATENAME(MONTH,event_date)
FROM
(
    SELECT CAST(transaction_initiation_date AS DATE) event_date
    FROM silver.disputed_pp01_transactions

    UNION

    SELECT CAST(create_time AS DATE)
    FROM silver.disputed_pp02_disputes
) d
WHERE event_date IS NOT NULL
AND NOT EXISTS
(
    SELECT 1
    FROM gold.dim_date g
    WHERE g.full_date = d.event_date
);
GO
