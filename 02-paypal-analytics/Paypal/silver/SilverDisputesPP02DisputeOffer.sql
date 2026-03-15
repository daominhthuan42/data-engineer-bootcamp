/*
Purpose:
Load and normalize dispute offer information from bronze.dispute_pp02
into silver.disputed_pp02_dispute_offer.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   most recent record based on ingestion partitions (dt, hour).
3. Extract buyer requested refund amount from the JSON column offer
   using JSON_VALUE.
4. Convert requested_amount from JSON string to DECIMAL using TRY_CAST.
5. Load structured dispute offer data into the Silver layer.

This step represents the Bronze → Silver transformation where
semi-structured dispute offer JSON data is converted into a
structured dataset for analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_offer', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_offer;
GO
INSERT INTO silver.disputed_pp02_dispute_offer (
    dispute_id,
    currency_code,
    requested_amount
)
SELECT
    t.dispute_id,
    JSON_VALUE(t.offer,'$.buyer_requested_amount.currency_code'),
    TRY_CAST(JSON_VALUE(t.offer,'$.buyer_requested_amount.value') AS DECIMAL(18,2))
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
