/*
Purpose:
Populate the Gold fact table gold.fact_disputePP01_transaction with
transaction-level dispute data by integrating transaction, payer,
shipping, and item aggregation information from the Silver layer.

Processing logic:
1. Check whether the target fact table gold.fact_disputePP01_transaction exists.
2. If the table exists, remove all existing records using TRUNCATE TABLE
   to allow a full reload of the fact dataset.
3. Create a CTE (CTE_ItemAgg) to aggregate item-level data from
   silver.disputed_pp01_items by transaction_id, calculating:
   - item_count: number of items in each transaction.
   - total_item_amount: total monetary value of all items.
4. Extract transaction-level data from silver.disputed_pp01_transactions
   including transaction attributes, balances, and currency details.
5. Generate a surrogate date_key in YYYYMMDD format from the transaction
   date column (dt) for use in a dimensional model.
6. Join payer information from silver.disputed_pp01_payer and map it to
   the Gold payer dimension (gold.dim_disputePP01_payer) to obtain
   the payer_key surrogate key.
7. Join shipping information from silver.disputed_pp01_shipping and map
   it to the Gold shipping dimension (gold.dim_disputePP01_shipping)
   to obtain the shipping_key surrogate key.
8. Join the aggregated item metrics from CTE_ItemAgg to enrich the
   transaction record with item-level summary measures.
9. Use LEFT JOIN operations to ensure that transactions without related
   payer, shipping, or item records are still loaded into the fact table.
10. Insert the resulting integrated dataset into the Gold fact table.

This step represents the final Silver → Gold transformation in the
Medallion architecture, where curated transactional data is modeled
as a fact table within a star schema to support analytical reporting,
dispute analysis, and business intelligence workloads.
*/

IF OBJECT_ID('gold.fact_disputePP01_transaction', 'U') IS NOT NULL
    TRUNCATE TABLE gold.fact_disputePP01_transaction;
GO
WITH CTE_ItemAgg AS
(
    SELECT      transaction_id,
                COUNT(*) AS item_count,
                SUM(COALESCE(item_amount,0)) AS total_item_amount
    FROM        silver.disputed_pp01_items
    GROUP BY    transaction_id
)
INSERT INTO gold.fact_disputePP01_transaction
(
            transaction_id,
            date_key,
            payer_key,
            shipping_key,
            transaction_event_code,
            transaction_status,
            transaction_currency,
            transaction_amount,
            ending_balance,
            available_balance,
            item_count,
            total_item_amount
)
SELECT      t.transaction_id,
            CONVERT(INT, CONVERT(VARCHAR(8), t.dt, 112)) AS date_key,
            p.payer_key,
            s.shipping_key,
            t.transaction_event_code,
            t.transaction_status,
            t.transaction_currency,
            t.transaction_amount,
            t.ending_balance,
            t.available_balance,
            COALESCE(i.item_count,0),
            COALESCE(i.total_item_amount,0)
FROM        silver.disputed_pp01_transactions t
LEFT JOIN   silver.disputed_pp01_payer sp ON t.transaction_id = sp.transaction_id
LEFT JOIN   gold.dim_disputePP01_payer p ON sp.account_id = p.account_id
LEFT JOIN   silver.disputed_pp01_shipping ss ON t.transaction_id = ss.transaction_id
LEFT JOIN   gold.dim_disputePP01_shipping s ON ss.line1 = s.line1
LEFT JOIN   CTE_ItemAgg i ON t.transaction_id = i.transaction_id;
GO
