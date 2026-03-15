/*
Purpose:
Populate the Gold dimension table gold.dim_disputePP02_dispute with
unique dispute records sourced from the Silver layer table
silver.disputed_pp02_disputes.

Processing logic:
1. Verify whether the target dimension table gold.dim_disputePP02_dispute exists.
2. If the table exists, remove all existing records using TRUNCATE TABLE
   to support a full dimension reload.
3. Extract dispute-related attributes from the Silver layer table
   silver.disputed_pp02_disputes, including dispute identifier, reason,
   status, dispute state, lifecycle stage, and dispute channel.
4. Ensure that duplicate dispute records are not inserted by validating
   that the dispute_id does not already exist in the target dimension
   table using a NOT EXISTS condition.
5. Insert the resulting distinct dispute records into the Gold dimension
   table to provide a structured dimension for dispute-related analytics.

This step represents the Silver → Gold transformation in the Medallion
architecture, where curated dispute metadata is consolidated into a
dimension table that supports analytical reporting, dispute lifecycle
tracking, and star schema modeling.
*/

IF OBJECT_ID('gold.dim_disputePP02_dispute', 'U') IS NOT NULL
    TRUNCATE TABLE gold.dim_disputePP02_dispute;
GO
INSERT INTO gold.dim_disputePP02_dispute
(
    dispute_id,
    reason,
    status,
    dispute_state,
    dispute_life_cycle_stage,
    dispute_channel
)
SELECT
    dispute_id,
    reason,
    status,
    dispute_state,
    dispute_life_cycle_stage,
    dispute_channel
FROM silver.disputed_pp02_disputes d
WHERE NOT EXISTS
(
    SELECT 1
    FROM gold.dim_disputePP02_dispute g
    WHERE g.dispute_id = d.dispute_id
);
GO
