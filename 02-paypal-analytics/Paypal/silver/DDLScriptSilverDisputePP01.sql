/*
===============================================================================
DDL Script: Create Silver Tables for disputed_pp01
===============================================================================
*/

USE [PaypalAnalytic];
GO

IF OBJECT_ID('silver.disputed_pp01_transactions', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp01_transactions;
GO
CREATE TABLE silver.disputed_pp01_transactions
(
    transaction_id              VARCHAR(50)     NOT NULL,
    transaction_event_code      VARCHAR(10)     NULL,
    transaction_initiation_date DATETIME2       NULL,
    transaction_updated_date    DATETIME2       NULL,
    transaction_currency        VARCHAR(10)     NULL,
    transaction_amount          DECIMAL(18,2)   NULL,
    ending_balance_currency     VARCHAR(10)     NULL,
    ending_balance              DECIMAL(18,2)   NULL,
    available_balance_currency  VARCHAR(10)     NULL,
    available_balance           DECIMAL(18,2)   NULL,
    transaction_status          VARCHAR(50)     NULL,
    transaction_subject         VARCHAR(255)    NULL,
    protection_eligibility      VARCHAR(10)     NULL,
    elton_created_at            DATETIME2       NULL,
    dt                          DATE            NULL,
    hour                        INT             NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_Transactions_DWHCreateDate DEFAULT SYSDATETIME()
    CONSTRAINT PK_Transactions_TransactionId  PRIMARY KEY(transaction_id),
    CONSTRAINT CK_Transactions_TransactionStatus CHECK(transaction_status IN ('D', 'P', 'S')),
    CONSTRAINT CK_Transactions_ProtectionEligibility CHECK(protection_eligibility IN (1, 2)),
    CONSTRAINT CK_Transactions_Hour CHECK(hour >= 0)
);
GO

IF OBJECT_ID('silver.disputed_pp01_payer', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp01_payer;
GO
CREATE TABLE silver.disputed_pp01_payer
(
    transaction_id              VARCHAR(50)     NOT NULL,
    account_id                  VARCHAR(50)     NULL,
    email_address               VARCHAR(255)    NULL,
    address_status              VARCHAR(5)      NULL,
    payer_status                VARCHAR(5)      NULL,
    given_name                  VARCHAR(100)    NULL,
    surname                     VARCHAR(100)    NULL,
    middle_name                 VARCHAR(100)    NULL,
    alternate_full_name         VARCHAR(200)    NULL,
    country_code                VARCHAR(10)     NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_Payer_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_Payer_PayerStatus CHECK(payer_status IN ('Y', 'N')),
    CONSTRAINT CK_Payer_AddressStatus CHECK(payer_status IN ('Y', 'N')),
    CONSTRAINT CK_Payer_EmailAddress CHECK(email_address LIKE '%_@_%._%')
);
GO

IF OBJECT_ID('silver.disputed_pp01_shipping', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp01_shipping;
GO
CREATE TABLE silver.disputed_pp01_shipping
(
    transaction_id              VARCHAR(50)     NOT NULL,
    name                        VARCHAR(255)    NULL,
    line1                       VARCHAR(255)    NULL,
    city                        VARCHAR(100)    NULL,
    country_code                VARCHAR(10)     NULL,
    postal_code                 VARCHAR(20)     NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_Shipping_DWHCreateDate DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.disputed_pp01_items', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp01_items;
GO
CREATE TABLE silver.disputed_pp01_items
(
    transaction_id              VARCHAR(50)     NOT NULL,
    item_name                   VARCHAR(255)    NULL,
    item_code                   VARCHAR(100)    NULL,
    invoice_number              VARCHAR(100)    NULL,
    item_quantity               DECIMAL(10,3)   NULL,
    tax_percentage              DECIMAL(5,2)    NULL,
    currency_code               VARCHAR(10)     NULL,
    item_unit_price             DECIMAL(18,2)   NULL,
    item_amount                 DECIMAL(18,2)   NULL,
    total_item_amount           DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_Items_DWHCreateDate DEFAULT SYSDATETIME()
);
GO
