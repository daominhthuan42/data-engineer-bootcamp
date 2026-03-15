/*
Purpose:
Load and normalize disputed transaction details from bronze.dispute_pp02
into silver.disputed_pp02_disputed_transactions.

Processing logic:
1. Truncate the target Silver table to allow a full reload.
2. Deduplicate records by dispute_id using ROW_NUMBER(), keeping the latest
   record based on ingestion partitions (dt, hour).
3. Parse the JSON array disputed_transactions using OPENJSON to generate
   one row per transaction.
4. Extract transaction attributes including buyer/seller identifiers,
   status, timestamps, and protection information.
5. Convert JSON string values to structured types:
   - create_time → DATETIME2
   - gross_amount.value → DECIMAL
   - seller_protection_eligible → BIT
6. Load the flattened transaction-level dispute data into the Silver layer.

This step represents the Bronze → Silver transformation where nested
disputed transaction JSON data is structured into relational records
for downstream analytics.
*/

IF OBJECT_ID('silver.disputed_pp02_disputed_transactions', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp02_disputed_transactions;
GO
INSERT INTO silver.disputed_pp02_disputed_transactions (
    dispute_id,
    buyer_transaction_id,
    seller_transaction_id,
    create_time,
    transaction_status,
    gross_amount_currency,
    gross_amount_value,
    custom,
    buyer_name,
    seller_email,
    seller_merchant_id,
    seller_name,
    seller_protection_eligible,
    seller_protection_type
)
SELECT
    t.dispute_id,
    j.buyer_transaction_id,
    j.seller_transaction_id,
    TRY_CAST(REPLACE(j.create_time,'Z','') AS DATETIME2),
    j.transaction_status,
    j.gross_currency,
    TRY_CAST(j.gross_value AS DECIMAL(18,2)),
    j.custom,
    j.buyer_name,
    j.seller_email,
    j.seller_merchant_id,
    j.seller_name,
    TRY_CAST(j.seller_protection_eligible AS BIT),
    j.seller_protection_type
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
    buyer_transaction_id        NVARCHAR(50)    '$.buyer_transaction_id',
    seller_transaction_id       NVARCHAR(50)    '$.seller_transaction_id',
    create_time                 NVARCHAR(50)    '$.create_time',
    transaction_status          NVARCHAR(50)    '$.transaction_status',
    gross_currency              NVARCHAR(10)    '$.gross_amount.currency_code',
    gross_value                 NVARCHAR(50)    '$.gross_amount.value',
    custom                      NVARCHAR(255)   '$.custom',
    buyer_name                  NVARCHAR(255)   '$.buyer.name',
    seller_email                NVARCHAR(255)   '$.seller.email',
    seller_merchant_id          NVARCHAR(100)   '$.seller.merchant_id',
    seller_name                 NVARCHAR(255)   '$.seller.name',
    seller_protection_eligible  NVARCHAR(10)    '$.seller_protection_eligible',
    seller_protection_type      NVARCHAR(100)   '$.seller_protection_type'
) j
WHERE t.flag_last = 1;
GO
