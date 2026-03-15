/*
Purpose:
Load cleaned and deduplicated shipping information from bronze.dispute_pp01 into silver.disputed_pp01_shipping.

Processing logic:
1. Remove existing data in silver.shipping to allow a full reload.
2. Extract shipping-related attributes from the JSON column (shipping_info) using JSON_VALUE.
3. Flatten the nested address structure into relational columns 
   (line1, city, country_code, postal_code).
4. Deduplicate records by transaction_id using ROW_NUMBER(), keeping the most recent
   record based on ingestion partition columns (dt, hour).
5. Ensure only records with valid transaction_id are processed.
6. Populate the Silver layer with the latest shipping details associated with each transaction.

This step represents the Bronze → Silver transformation in the Medallion architecture,
where semi-structured shipping JSON data is normalized into structured relational columns
to support downstream analytics and integration with other transactional datasets.
*/

IF OBJECT_ID('silver.disputed_pp01_shipping', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp01_shipping;
GO
INSERT INTO [silver].[disputed_pp01_shipping] (
        transaction_id,
        name,
        line1,
        city,
        country_code,
        postal_code
)
SELECT  transaction_id,
        JSON_VALUE(shipping_info, '$.name'),
        JSON_VALUE(shipping_info, '$.address.line1'),
        JSON_VALUE(shipping_info, '$.address.city'),
        JSON_VALUE(shipping_info, '$.address.country_code'),
        JSON_VALUE(shipping_info, '$.address.postal_code')
FROM (
                SELECT  *,
                        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY dt DESC, hour DESC) AS flag_last
                FROM    bronze.dispute_pp01
                WHERE   transaction_id IS NOT NULL
) AS t
WHERE           t.flag_last = 1 -- Select the most recent record per customer
ORDER BY        transaction_id ASC;
GO
