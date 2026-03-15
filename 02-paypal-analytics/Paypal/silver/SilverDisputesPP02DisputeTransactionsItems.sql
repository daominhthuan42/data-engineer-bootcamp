/*
Purpose:
Load and normalize disputed transaction item details from bronze.dispute_pp02
into silver.disputed_pp02_disputed_transaction_items.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate source records by dispute_id using ROW_NUMBER(), keeping the
   most recent record based on ingestion partitions (dt, hour).
3. Parse the JSON array disputed_transactions using OPENJSON to extract
   buyer_transaction_id.
4. Expand the nested items array to generate one row per disputed item.
5. Extract item attributes such as name, description, quantity, reason,
   and item type from JSON fields.
6. Convert item_quantity from JSON string to DECIMAL using TRY_CAST.
7. Load the flattened item-level dispute data into the Silver layer.

This step represents the Bronze → Silver transformation where nested
disputed transaction JSON data is flattened into structured item-level
records for analytical processing.
*/

IF OBJECT_ID('silver.disputed_pp02_disputed_transaction_items', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_disputed_transaction_items;
GO
INSERT INTO silver.disputed_pp02_disputed_transaction_items (
    dispute_id,
    buyer_transaction_id,
    item_name,
    item_description,
    item_quantity,
    item_reason,
    item_item_type
)
SELECT
    t.dispute_id,
    tr.buyer_transaction_id,
    it.item_name,
    it.item_description,
    TRY_CAST(it.item_quantity AS DECIMAL(18,3)),
    it.reason,
    it.item_type
FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dispute_id
                   ORDER BY dt DESC, hour DESC
               ) AS flag_last
        FROM bronze.dispute_pp02
        WHERE dispute_id IS NOT NULL
) t
CROSS APPLY OPENJSON(t.disputed_transactions)
WITH (
    buyer_transaction_id NVARCHAR(50) '$.buyer_transaction_id',
    items NVARCHAR(MAX) AS JSON
) tr
CROSS APPLY OPENJSON(tr.items)
WITH (
    item_name           NVARCHAR(255)   '$.item_name',
    item_description    NVARCHAR(255)   '$.item_description',
    item_quantity       NVARCHAR(50)    '$.item_quantity',
    reason              NVARCHAR(100)   '$.reason',
    item_type           NVARCHAR(50)    '$.item_type'
) it

WHERE t.flag_last = 1;
GO
