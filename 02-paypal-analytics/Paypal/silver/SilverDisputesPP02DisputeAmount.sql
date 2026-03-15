/*
Purpose:
Load and normalize dispute amount information from bronze.dispute_pp02
into silver.disputed_pp02_dispute_amount.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Extract dispute amount attributes from the JSON column dispute_amount
   using JSON_VALUE.
4. Convert the amount value from JSON string to DECIMAL using TRY_CAST.
5. Load structured dispute amount data into the Silver layer.

This step represents the Bronze → Silver transformation where
semi-structured dispute amount JSON data is converted into a
structured dataset for financial analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_amount', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_amount;
GO
INSERT INTO silver.disputed_pp02_dispute_amount (
    dispute_id,
    currency_code,
    amount_value
)
SELECT
    t.dispute_id,
    JSON_VALUE(t.dispute_amount, '$.currency_code') AS currency_code,
    TRY_CAST(JSON_VALUE(t.dispute_amount, '$.value') AS DECIMAL(18,2)) AS amount_value
FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dispute_id
                   ORDER BY dt DESC, hour DESC
               ) AS flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t
WHERE t.flag_last = 1;
GO
