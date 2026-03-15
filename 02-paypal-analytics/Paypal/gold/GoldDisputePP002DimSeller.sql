/*
Purpose:
Populate the Gold dimension table gold.dim_disputePP02_Seller with unique
seller records derived from the Silver layer table
silver.disputed_pp02_disputed_transactions.

Processing logic:
1. Check whether the target dimension table gold.dim_disputePP02_Seller exists.
2. If the table exists, remove all existing data using TRUNCATE TABLE to
   allow a full reload of the seller dimension.
3. Extract distinct seller attributes from the Silver layer table
   silver.disputed_pp02_disputed_transactions, including merchant ID,
   seller email, seller name, and seller protection details.
4. Exclude records where seller_merchant_id is NULL to ensure that only
   valid seller identifiers are loaded into the dimension table.
5. Prevent duplicate seller entries by verifying that the
   seller_merchant_id does not already exist in the target dimension
   using a NOT EXISTS condition.
6. Insert the cleaned and deduplicated seller records into the
   Gold dimension table.

This step represents the Silver → Gold transformation in the Medallion
architecture, where curated seller information is consolidated into
a dimension table used for dispute analysis, seller performance
tracking, and star schema modeling.
*/

IF OBJECT_ID('gold.dim_disputePP02_Seller', 'U') IS NOT NULL
    TRUNCATE TABLE gold.dim_disputePP02_Seller;
GO
INSERT INTO gold.dim_disputePP02_Seller
(
    seller_merchant_id,
    seller_email,
    seller_name,
    seller_protection_type,
    seller_protection_eligible
)
SELECT DISTINCT
    seller_merchant_id,
    seller_email,
    seller_name,
    seller_protection_type,
    seller_protection_eligible
FROM silver.disputed_pp02_disputed_transactions s
WHERE seller_merchant_id IS NOT NULL
AND NOT EXISTS
(
    SELECT 1
    FROM gold.dim_disputePP02_Seller d
    WHERE d.seller_merchant_id = s.seller_merchant_id
);
GO
