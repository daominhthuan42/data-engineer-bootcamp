/*
Purpose:
Populate the Gold dimension table gold.dim_disputePP01_shipping with unique
shipping address records sourced from the Silver layer table
silver.disputed_pp01_shipping.

Processing logic:
1. Check whether the target dimension table gold.dim_disputePP01_shipping exists.
2. If the table exists, remove all existing records using TRUNCATE TABLE to
   support a full reload of the dimension.
3. Retrieve distinct shipping address attributes from the Silver layer,
   including recipient name, address line, city, country code, postal code,
   and the data warehouse creation timestamp.
4. Ensure that duplicate shipping records are not inserted by validating that
   the combination of line1 (street address) and postal_code does not already
   exist in the target dimension table using a NOT EXISTS condition.
5. Insert the cleaned and deduplicated shipping address records into the
   Gold dimension table for use in analytical queries and reporting.

This step represents the Silver → Gold transformation in the Medallion
architecture, where curated shipping address data is consolidated into a
dimension table to support dispute analysis and star schema modeling.
*/

IF OBJECT_ID('gold.dim_disputePP01_shipping', 'U') IS NOT NULL
    TRUNCATE TABLE gold.dim_disputePP01_shipping;
GO
INSERT INTO gold.dim_disputePP01_shipping
(
    name,
    line1,
    city,
    country_code,
    postal_code,
    dwh_create_date
)
SELECT DISTINCT
    name,
    line1,
    city,
    country_code,
    postal_code,
    dwh_create_date
FROM silver.disputed_pp01_shipping s
WHERE NOT EXISTS
(
    SELECT 1
    FROM gold.dim_disputePP01_shipping d
    WHERE d.line1 = s.line1
    AND d.postal_code = s.postal_code
);
GO
