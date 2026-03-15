/*
Purpose:
Load and normalize dispute evidence records from bronze.dispute_pp02
into silver.disputed_pp02_dispute_evidences.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Parse the evidences JSON object using OPENJSON to extract
   evidence attributes.
4. Retrieve fields such as evidence_type, notes, source,
   dispute_life_cycle_stage, and evidence_date.
5. Convert evidence_date from JSON string to DATETIME2.
6. Load structured evidence records into the Silver layer.

This step represents the Bronze → Silver transformation where
semi-structured evidence JSON data is converted into a
structured dataset for dispute analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_evidences;
GO
INSERT INTO silver.disputed_pp02_dispute_evidences (
    dispute_id,
    evidence_type,
    notes,
    source,
    evidence_date,
    dispute_life_cycle_stage
)
SELECT
    t.dispute_id,
    j.evidence_type,
    j.notes,
    j.source,
    TRY_CAST(REPLACE(j.evidence_date,'Z','') AS DATETIME2),
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
CROSS APPLY OPENJSON(t.evidences)
WITH (
    evidence_type               NVARCHAR(100)   '$.evidence_type',
    notes                       NVARCHAR(MAX)   '$.notes',
    source                      NVARCHAR(100)   '$.source',
    evidence_date               NVARCHAR(50)    '$.date',
    dispute_life_cycle_stage    NVARCHAR(100)   '$.dispute_life_cycle_stage'
) j
WHERE t.flag_last = 1;
GO
