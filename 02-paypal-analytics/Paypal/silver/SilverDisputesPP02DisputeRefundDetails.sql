/*
Purpose:
Load and normalize refund detail attributes from bronze.dispute_pp02
into silver.disputed_pp02_refund_details.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   most recent record based on ingestion partitions (dt, hour).
3. Extract refund details from the JSON column refund_details using JSON_VALUE.
4. Convert allowed_refund_amount.value from JSON string to DECIMAL
   using TRY_CAST.
5. Load structured refund amount information into the Silver layer.

This step represents the Bronze → Silver transformation where
semi-structured refund detail JSON data is converted into a
structured dataset for downstream analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_refund_details', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_refund_details;
GO
INSERT INTO silver.disputed_pp02_refund_details (
    dispute_id,
    currency_code,
    allowed_refund_amount
)
SELECT
    t.dispute_id,
    JSON_VALUE(t.refund_details,'$.allowed_refund_amount.currency_code'),
    TRY_CAST(JSON_VALUE(t.refund_details,'$.allowed_refund_amount.value') AS DECIMAL(18,2))

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
