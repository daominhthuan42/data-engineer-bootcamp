/*
Purpose:
Load and normalize dispute evidence tracking information from
bronze.dispute_pp02 into silver.disputed_pp02_dispute_evidences_tracking.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Parse the evidences JSON object using OPENJSON.
4. Extract the nested tracking_info array and expand it into
   individual tracking records.
5. Retrieve carrier_name and tracking_number for each shipment entry.
6. Load the flattened tracking evidence data into the Silver layer.

This step represents the Bronze → Silver transformation where nested
evidence tracking JSON data is converted into structured shipment
tracking records for analysis and investigation.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences_tracking', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_evidences_tracking;
GO
INSERT INTO silver.disputed_pp02_dispute_evidences_tracking (
    dispute_id,
    carrier_name,
    tracking_number
)
SELECT
    t.dispute_id,
    tr.carrier_name,
    tr.tracking_number

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
    tracking_info   NVARCHAR(MAX) '$.evidence_info.tracking_info' AS JSON
) e
CROSS APPLY OPENJSON(e.tracking_info)
WITH (
    carrier_name    NVARCHAR(100) '$.carrier_name',
    tracking_number NVARCHAR(100) '$.tracking_number'
) tr
WHERE t.flag_last = 1;
GO
