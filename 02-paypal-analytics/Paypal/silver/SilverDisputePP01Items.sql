/*
Purpose:
Load cleaned and deduplicated item-level transaction details from bronze.dispute_pp01 into silver.disputed_pp01_items.

Processing logic:
1. Remove existing data in silver.items to allow a full reload.
2. Deduplicate source records by transaction_id using ROW_NUMBER(), keeping the most recent
   record based on ingestion partition columns (dt, hour).
3. Parse the JSON array cart_info.item_details using OPENJSON to expand each item into
   a separate relational row.
4. Extract item attributes such as item_name, item_code, invoice_number, quantity,
   tax percentage, and pricing information.
5. Convert numeric values stored as JSON strings into appropriate DECIMAL data types
   using TRY_CAST to prevent load failures from malformed values.
6. Populate the Silver layer with normalized item-level transaction data where
   each transaction may contain multiple item records.

This step represents the Bronze → Silver transformation in the Medallion architecture,
where nested semi-structured cart JSON data is flattened into a structured
transaction-item dataset suitable for analytical processing and aggregation.
*/

IF OBJECT_ID('silver.disputed_pp01_items', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp01_items;
GO
INSERT INTO silver.disputed_pp01_items (
    transaction_id,
    item_name,
    item_code,
    invoice_number,
    item_quantity,
    tax_percentage,
    currency_code,
    item_unit_price,
    item_amount,
    total_item_amount
)
SELECT  
    t.transaction_id,
    i.item_name,
    i.item_code,
    i.invoice_number,
    TRY_CAST(i.item_quantity AS DECIMAL(18,3)) AS item_quantity,
    TRY_CAST(i.tax_percentage AS DECIMAL(10,2)) AS tax_percentage,
    i.currency_code,
    TRY_CAST(i.item_unit_price AS DECIMAL(10,2)) AS item_unit_price,
    TRY_CAST(i.item_amount AS DECIMAL(18,2)) AS item_amount,
    TRY_CAST(i.total_item_amount AS DECIMAL(18,2)) AS total_item_amount
FROM (
        SELECT  *,
                ROW_NUMBER() OVER (
                    PARTITION BY transaction_id 
                    ORDER BY dt DESC, hour DESC
                ) AS flag_last
        FROM bronze.dispute_pp01
        WHERE transaction_id IS NOT NULL
     ) t
CROSS APPLY OPENJSON(t.cart_info, '$.item_details')
WITH (
    item_name           NVARCHAR(255)   '$.item_name',
    item_code           NVARCHAR(50)    '$.item_code',
    invoice_number      NVARCHAR(100)   '$.invoice_number',
    item_quantity       NVARCHAR(30)    '$.item_quantity',
    tax_percentage      NVARCHAR(30)    '$.tax_percentage',
    currency_code       NVARCHAR(10)    '$.item_unit_price.currency_code',
    item_unit_price     NVARCHAR(50)    '$.item_unit_price.value',
    item_amount         NVARCHAR(50)    '$.item_amount.value',
    total_item_amount   NVARCHAR(50)    '$.total_item_amount.value'
) i
WHERE t.flag_last = 1;
GO
