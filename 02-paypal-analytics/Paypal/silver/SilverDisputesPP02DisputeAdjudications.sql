/*
Purpose:
Load and normalize dispute adjudication records from bronze.dispute_pp02
into silver.disputed_pp02_dispute_adjudications.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Parse the adjudications JSON array using OPENJSON to generate
   one row per adjudication event.
4. Extract adjudication attributes including type, reason,
   dispute life cycle stage, and adjudication time.
5. Convert adjudication_time from JSON string to DATETIME2.
6. Load structured adjudication records into the Silver layer.

This step represents the Bronze → Silver transformation where
nested adjudication JSON data is flattened into structured
dispute decision records for analysis and auditing.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_adjudications', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_adjudications;
GO
INSERT INTO silver.disputed_pp02_dispute_adjudications (
    dispute_id,
    adjudication_type,
    adjudication_time,
    reason,
    dispute_life_cycle_stage
)
SELECT
    t.dispute_id,
    j.adjudication_type,
    TRY_CAST(REPLACE(j.adjudication_time,'Z','') AS DATETIME2),
    j.reason,
    j.dispute_life_cycle_stage
FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dispute_id
                   ORDER BY dt DESC, hour DESC
               ) AS flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t
CROSS APPLY OPENJSON(t.adjudications)
WITH (
    adjudication_type           NVARCHAR(100) '$.type',
    adjudication_time           NVARCHAR(50)  '$.adjudication_time',
    reason                      NVARCHAR(255) '$.reason',
    dispute_life_cycle_stage    NVARCHAR(100) '$.dispute_life_cycle_stage'
) j
WHERE t.flag_last = 1;
GO
