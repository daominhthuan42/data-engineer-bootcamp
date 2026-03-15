/*
Purpose:
Populate the Gold fact table gold.fact_disputePP02_dispute with dispute-level
analytical metrics by integrating dispute details, seller information,
transaction activity, and financial movement data from the Silver layer.

Processing logic:
1. Check whether the target fact table gold.fact_disputePP02_dispute exists.
2. If the table exists, remove all existing records using TRUNCATE TABLE
   to allow a full reload of the fact dataset.
3. Extract dispute records from silver.disputed_pp02_disputes and join
   them with the Gold dispute dimension (gold.dim_disputePP02_dispute)
   to obtain the surrogate dispute_key.
4. Generate a date_key in YYYYMMDD format from the dispute create_time
   column to align with the data warehouse date dimension structure.
5. Join dispute transaction details from
   silver.disputed_pp02_disputed_transactions and map seller information
   to the seller dimension (gold.dim_disputePP02_Seller) to retrieve
   the seller_key surrogate key.
6. Join dispute monetary values from silver.disputed_pp02_dispute_amount
   to capture the dispute_amount associated with each dispute case.
7. Join financial movement records from
   silver.disputed_pp02_dispute_money_movements and aggregate the
   movement amounts to compute total money_movement_amount.
8. Calculate transaction_count as the number of distinct buyer
   transactions involved in each dispute.
9. Determine seller_protection_eligible by evaluating the maximum
   eligibility flag across related transactions and converting it
   to a BIT value for the fact table.
10. Aggregate and group the data by dispute_key, dispute creation date,
    seller_key, and dispute_amount to produce a dispute-level fact record.

This step represents the final Silver → Gold transformation in the
Medallion architecture, where curated dispute-related data is modeled
as a fact table within a star schema to support analytical reporting,
financial impact analysis, and dispute performance monitoring.
*/

IF OBJECT_ID('gold.fact_disputePP02_dispute', 'U') IS NOT NULL
    TRUNCATE TABLE gold.fact_disputePP02_dispute;
GO
INSERT INTO gold.fact_disputePP02_dispute
(
            dispute_key,
            date_key,
            seller_key,
            dispute_amount,
            transaction_count,
            money_movement_amount,
            seller_protection_eligible
)
SELECT      dd.dispute_key,
            CONVERT(INT, CONVERT(VARCHAR(8), d.create_time,112)),
            s.seller_key,
            COALESCE(da.amount_value,0),
            COUNT(DISTINCT dt.buyer_transaction_id),
            SUM(COALESCE(mm.amount,0)),
            CAST(MAX(CAST(dt.seller_protection_eligible AS INT)) AS BIT)
FROM        silver.disputed_pp02_disputes d
LEFT JOIN   gold.dim_disputePP02_dispute dd ON d.dispute_id = dd.dispute_id
LEFT JOIN   silver.disputed_pp02_dispute_amount da ON d.dispute_id = da.dispute_id
LEFT JOIN   silver.disputed_pp02_disputed_transactions dt ON d.dispute_id = dt.dispute_id
LEFT JOIN   gold.dim_disputePP02_Seller s ON dt.seller_merchant_id = s.seller_merchant_id
LEFT JOIN   silver.disputed_pp02_dispute_money_movements mm ON d.dispute_id = mm.dispute_id
GROUP BY    dd.dispute_key,
            d.create_time,
            s.seller_key,
            da.amount_value;
GO
