/*
Purpose:
Load and normalize dispute extension attributes from bronze.dispute_pp02 into
silver.disputed_pp02_dispute_extensions.

Processing logic:
1. Truncate the target Silver table to perform a full reload.
2. Deduplicate source records by dispute_id using ROW_NUMBER(), keeping the latest
   record based on ingestion partitions (dt, hour).
3. Extract extension fields from the JSON column extensions using JSON_VALUE.
4. Convert JSON string values into structured data types:
   - buyer_contacted_time → DATETIME2
   - correct_transaction_amount.value → DECIMAL
5. Load cleaned and structured dispute extension data into the Silver layer.

This step represents the Bronze → Silver transformation where semi-structured
dispute extension JSON data is flattened into a structured dataset for analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_extensions', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_extensions;
GO
INSERT INTO silver.disputed_pp02_dispute_extensions (
    dispute_id,
    merchant_contacted_outcome,
    buyer_contacted_time,
    correct_amount_currency,
    correct_amount_value
)
SELECT
    t.dispute_id,
    JSON_VALUE(t.extensions,'$.merchant_contacted_outcome'),
    TRY_CAST(REPLACE(JSON_VALUE(t.extensions,'$.buyer_contacted_time'),'Z','') AS DATETIME2),
    JSON_VALUE(t.extensions,'$.correct_transaction_amount.currency_code'),
    TRY_CAST(JSON_VALUE(t.extensions,'$.correct_transaction_amount.value') AS DECIMAL(18,2))
FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dispute_id
                   ORDER BY dt DESC, hour DESC
               ) flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t
WHERE t.flag_last = 1;
GO
