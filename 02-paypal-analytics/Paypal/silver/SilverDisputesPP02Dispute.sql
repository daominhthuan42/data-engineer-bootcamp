/*
Purpose:
Load and normalize core dispute records from bronze.dispute_pp02
into silver.disputed_pp02_disputes.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Extract core dispute attributes including reason, status,
   dispute state, lifecycle stage, and dispute channel.
4. Convert timestamp fields (create_time, update_time,
   seller_response_due_date, elton_created_at) to DATETIME2.
5. Convert partition fields dt and hour to DATE and INT.
6. Load the cleaned dispute-level dataset into the Silver layer.

This step represents the Bronze → Silver transformation where
raw dispute event data is standardized into a structured
dispute-level table for downstream analytics and reporting.
*/

IF OBJECT_ID('silver.disputed_pp02_disputes', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_disputes;
GO
INSERT INTO silver.disputed_pp02_disputes (
    dispute_id,
    create_time,
    update_time,
    reason,
    status,
    dispute_state,
    dispute_life_cycle_stage,
    dispute_channel,
    seller_response_due_date,
    elton_created_at,
    dt,
    hour
)
SELECT
    t.dispute_id,
    TRY_CAST(REPLACE(create_time,' UTC','') AS DATETIME2),
    TRY_CAST(REPLACE(update_time,' UTC','') AS DATETIME2),
    t.reason,
    t.status,
    t.dispute_state,
    t.dispute_life_cycle_stage,
    t.dispute_channel,
    TRY_CAST(t.seller_response_due_date AS DATETIME2),
    TRY_CAST(REPLACE(elton_created_at,' UTC','') AS DATETIME2),
    TRY_CAST(t.dt AS DATE),
    TRY_CAST(t.hour AS INT)
FROM (
        SELECT  *,
                ROW_NUMBER() OVER (
                    PARTITION BY dispute_id
                    ORDER BY dt DESC, hour DESC
                ) AS flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t
WHERE t.flag_last = 1;
GO
