/*
Purpose:
Populate the Gold dimension table gold.dim_disputePP01_item with unique item
records derived from silver.disputed_pp01_items.

Processing logic:
1. Check whether the target dimension table gold.dim_disputePP01_item exists.
2. If the table exists, remove all existing records using TRUNCATE TABLE to
   allow a full dimension reload.
3. Extract distinct item_code and item_name values from the Silver layer
   table silver.disputed_pp01_items.
4. Filter out records where item_code is NULL to ensure valid dimension keys.
5. Prevent duplicate inserts by verifying that the item_code does not already
   exist in the target dimension table using NOT EXISTS.
6. Insert the cleaned and deduplicated item records into the Gold dimension
   table with placeholder NULL values for item_description and item_type,
   which may be populated later through enrichment or business classification.

This step represents the Silver → Gold transformation in the Medallion
architecture, where curated item data is consolidated into a dimension
table that supports analytical queries, reporting, and star schema modeling.
*/

IF OBJECT_ID('gold.dim_disputePP01_item', 'U') IS NOT NULL
    TRUNCATE TABLE gold.dim_disputePP01_item;
GO
INSERT INTO gold.dim_disputePP01_item
(
    item_code,
    item_name,
    item_description,
    item_type
)
SELECT DISTINCT
    item_code,
    item_name,
    NULL,
    NULL
FROM silver.disputed_pp01_items
WHERE item_code IS NOT NULL
AND NOT EXISTS
(
    SELECT 1
    FROM gold.dim_disputePP01_item d
    WHERE d.item_code = silver.disputed_pp01_items.item_code
);
GO
