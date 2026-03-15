/*
Purpose:
Load and normalize dispute money movement records from bronze.dispute_pp02
into silver.disputed_pp02_dispute_money_movements.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   most recent record based on ingestion partitions (dt, hour).
3. Parse the JSON array adjudications using OPENJSON to generate
   one row per money movement.
4. Extract attributes including affected party, amount, currency,
   movement type, reason, and initiated time.
5. Convert initiated_time from JSON string to DATETIME2.
6. Load structured dispute money movement data into the Silver layer.

This step represents the Bronze → Silver transformation where
nested adjudication JSON data is flattened into structured
financial movement records for downstream analysis.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_money_movements', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_money_movements;
GO
INSERT INTO silver.disputed_pp02_dispute_money_movements (
        dispute_id,
        affected_party,
        currency_code,
        amount,
        initiated_time,
        movement_type,
        reason
)
SELECT  t.dispute_id,
        j.affected_party,
        j.currency_code,
        j.amount,
        TRY_CAST(REPLACE(j.initiated_time,'Z','') AS DATETIME2),
        j.movement_type,
        j.reason
FROM (
        SELECT  *,
                ROW_NUMBER() OVER (PARTITION BY dispute_id ORDER BY dt DESC, hour DESC) AS flag_last
        FROM    bronze.dispute_pp02
        WHERE   dispute_id IS NOT NULL
) t
CROSS APPLY OPENJSON(t.adjudications)
WITH (
    affected_party  NVARCHAR(50)    '$.affected_party',
    currency_code   NVARCHAR(10)    '$.amount.currency_code',
    amount          DECIMAL(18,2)   '$.amount.value',
    initiated_time  NVARCHAR(100)   '$.initiated_time',
    movement_type   NVARCHAR(50)    '$.type',
    reason          NVARCHAR(50)    '$.reason'
) j
WHERE t.flag_last = 1;
GO
