/*
Purpose:
Load and normalize dispute evidence document information from
bronze.dispute_pp02 into silver.disputed_pp02_dispute_evidences_documents.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the
   latest record based on ingestion partitions (dt, hour).
3. Parse the evidences JSON object using OPENJSON.
4. Extract the nested documents array and expand it into individual
   document records.
5. Retrieve document name and URL for each evidence document.
6. Load the flattened evidence document data into the Silver layer.

This step represents the Bronze → Silver transformation where nested
evidence document JSON data is converted into structured records
for dispute investigation and auditing.
*/

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences_documents', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_dispute_evidences_documents;
GO
INSERT INTO silver.disputed_pp02_dispute_evidences_documents (
    dispute_id,
    document_name,
    document_url
)
SELECT
    t.dispute_id,
    d.document_name,
    d.document_url
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
    documents NVARCHAR(MAX) '$.documents' AS JSON
) e
CROSS APPLY OPENJSON(e.documents)
WITH (
    document_name   NVARCHAR(255) '$.name',
    document_url    NVARCHAR(MAX) '$.url'
) d
WHERE t.flag_last = 1;
GO
