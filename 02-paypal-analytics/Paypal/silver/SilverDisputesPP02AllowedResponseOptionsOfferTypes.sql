/*
Purpose:
Load and normalize allowed offer response types from bronze.dispute_pp02
into silver.disputed_pp02_allowed_response_options_offer_types.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Parse the JSON column allowed_response_options using OPENJSON.
4. Extract offer_types under make_offer from the JSON structure.
5. Generate one row per allowed offer type for each dispute.
6. Load the structured offer response options into the Silver layer.

This step represents the Bronze → Silver transformation where
semi-structured allowed response option JSON data is normalized
into relational records for dispute response analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_allowed_response_options_offer_types', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_allowed_response_options_offer_types;
GO
INSERT INTO silver.disputed_pp02_allowed_response_options_offer_types (
    dispute_id,
    offer_type
)
SELECT
    t.dispute_id,
    o.offer_type
FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dispute_id
                   ORDER BY dt DESC, hour DESC
               ) flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t

CROSS APPLY OPENJSON(t.allowed_response_options)
WITH (
    offer_type NVARCHAR(50) '$.make_offer.offer_types'
) o
WHERE t.flag_last = 1;
GO
