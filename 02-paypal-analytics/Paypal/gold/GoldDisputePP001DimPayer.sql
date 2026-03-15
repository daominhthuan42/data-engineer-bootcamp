/*
Purpose:
Populate the Gold dimension table gold.dim_disputePP01_payer with unique
payer records sourced from the Silver layer table silver.disputed_pp01_payer.

Processing logic:
1. Verify whether the target dimension table gold.dim_disputePP01_payer exists.
2. If the table exists, remove all existing data using TRUNCATE TABLE to
   support a full dimension reload.
3. Select distinct payer-related attributes from the Silver layer table
   silver.disputed_pp01_payer, including account information, identity
   attributes, contact details, and status indicators.
4. Ensure that only unique payer records are inserted by checking that
   the account_id does not already exist in the target dimension table
   using a NOT EXISTS condition.
5. Insert the deduplicated payer records into the Gold dimension table,
   preserving attributes such as email, name components, country code,
   payer status, address status, and the data warehouse creation timestamp.

This step represents the Silver → Gold transformation in the Medallion
architecture, where cleaned payer data from the Silver layer is consolidated
into a dimension table used for analytical reporting, dispute analysis,
and star schema modeling.
*/

IF OBJECT_ID('gold.dim_disputePP01_payer', 'U') IS NOT NULL
    TRUNCATE TABLE gold.dim_disputePP01_payer;
GO
INSERT INTO gold.dim_disputePP01_payer
(
    account_id,
    email_address,
    given_name,
    surname,
    middle_name,
    alternate_full_name,
    country_code,
    payer_status,
    address_status,
    dwh_create_date
)
SELECT DISTINCT
    account_id,
    email_address,
    given_name,
    surname,
    middle_name,
    alternate_full_name,
    country_code,
    payer_status,
    address_status,
    dwh_create_date
FROM silver.disputed_pp01_payer p
WHERE NOT EXISTS
(
    SELECT 1
    FROM gold.dim_disputePP01_payer d
    WHERE d.account_id = p.account_id
);
GO
