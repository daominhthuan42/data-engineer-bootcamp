/*
Purpose:
Load cleaned and deduplicated transaction data from bronze.dispute_pp01 into silver.disputed_pp01_transactions.

Processing logic:
1. Remove existing data in silver.transactions to allow a full reload.
2. Extract and transform JSON fields (transaction_amount, ending_balance, available_balance)
   into structured relational columns.
3. Convert UTC timestamp strings into DATETIME2 format.
4. Deduplicate records by transaction_id using ROW_NUMBER(), keeping the most recent
   record based on ingestion partition columns (dt, hour).
5. Ensure only valid transaction_id records are processed.
6. Populate the Silver layer with the latest transactional state for each transaction_id.

This step represents the Bronze → Silver transformation in the Medallion architecture,
where semi-structured raw data is standardized, typed, and deduplicated for downstream
analytics and Gold layer consumption.
*/

IF OBJECT_ID('silver.disputed_pp01_transactions', 'U') IS NOT NULL
    TRUNCATE TABLE silver.disputed_pp01_transactions;
GO

INSERT INTO [silver].[disputed_pp01_transactions] (
                [transaction_id],
                [transaction_event_code],
                [transaction_initiation_date],
                [transaction_updated_date],
                [transaction_currency],
                [transaction_amount],
                [ending_balance_currency],
                [ending_balance],
                [available_balance_currency],
                [available_balance],
                [transaction_status],
                [transaction_subject],
                [protection_eligibility],
                [elton_created_at],
                [dt],
                [hour]
)
SELECT          transaction_id,
                transaction_event_code,
                TRY_CAST(REPLACE(transaction_initiation_date,' UTC','') AS DATETIME2),
                TRY_CAST(REPLACE(transaction_updated_date,' UTC','') AS DATETIME2),
                JSON_VALUE(transaction_amount, '$.currency_code'),
                CAST(JSON_VALUE(transaction_amount, '$.value') AS DECIMAL(18, 2)),
                JSON_VALUE(ending_balance, '$.currency_code'),
                CAST(JSON_VALUE(ending_balance, '$.value') AS DECIMAL(18, 2)),
                JSON_VALUE(available_balance, '$.currency_code'),
                CAST(JSON_VALUE(available_balance, '$.value') AS DECIMAL(18, 2)),
                transaction_status,
                transaction_subject,
                protection_eligibility,
                TRY_CAST(REPLACE(elton_created_at,' UTC','') AS DATETIME2),
                CAST(dt AS DATE),
                hour
FROM (
                SELECT  *,
                        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY dt DESC, hour DESC) AS flag_last
                FROM    bronze.dispute_pp01
                WHERE   transaction_id IS NOT NULL
) AS t
WHERE           t.flag_last = 1 -- Select the most recent record per customer
ORDER BY        transaction_id ASC;
GO
