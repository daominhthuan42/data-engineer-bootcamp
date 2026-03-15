/*
Purpose:
Load cleaned and deduplicated payer information from bronze.dispute_pp01 into silver.disputed_pp01_payer.

Processing logic:
1. Remove existing data in silver.payer to allow a full reload.
2. Extract payer-related attributes from the JSON column (payer_info) using JSON_VALUE.
3. Flatten nested JSON structures such as payer_name into relational columns 
   (given_name, surname, middle_name, alternate_full_name).
4. Deduplicate records by transaction_id using ROW_NUMBER(), keeping the most recent
   record based on ingestion partition columns (dt, hour).
5. Ensure only records with valid transaction_id are processed.
6. Populate the Silver layer with the latest payer attributes associated with each transaction.

This step represents the Bronze → Silver transformation in the Medallion architecture,
where semi-structured payer JSON data is normalized into structured relational columns
to support downstream analytics and integration with other transactional datasets.
*/

IF OBJECT_ID('silver.disputed_pp01_payer', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp01_payer;
GO
INSERT INTO [silver].[disputed_pp01_payer]
(
    transaction_id,
    account_id,
    email_address,
    given_name,
    surname,
    middle_name,
    alternate_full_name,
    address_status,
    payer_status,
    country_code
)
SELECT  transaction_id,
        JSON_VALUE(payer_info, '$.account_id'),
        JSON_VALUE(payer_info, '$.email_address'),
        JSON_VALUE(payer_info, '$.payer_name.given_name'),
        JSON_VALUE(payer_info, '$.payer_name.surname'),
        JSON_VALUE(payer_info, '$.payer_name.middle_name'),
        JSON_VALUE(payer_info, '$.payer_name.alternate_full_name'),
        JSON_VALUE(payer_info, '$.address_status'),
        JSON_VALUE(payer_info, '$.payer_status'),
        JSON_VALUE(payer_info, '$.country_code')
FROM (
                SELECT  *,
                        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY dt DESC, hour DESC) AS flag_last
                FROM    bronze.dispute_pp01
                WHERE   transaction_id IS NOT NULL
) AS t
WHERE           t.flag_last = 1 -- Select the most recent record per customer
ORDER BY        transaction_id ASC;
GO
