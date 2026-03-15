/*
===============================================================================
DDL Script: Create Gold Tables
gold
│
├── dim_date
├── dim_payer
├── dim_shipping
├── dim_item
├── dim_seller
├── dim_dispute
│
├── fact_transaction
└── fact_dispute
===============================================================================
*/

USE [PaypalAnalytic];
GO

IF OBJECT_ID('gold.dim_date','U') IS NOT NULL
DROP TABLE gold.dim_date;
GO
CREATE TABLE gold.dim_date
(
    date_key            INT PRIMARY KEY,   -- 20260101
    full_date           DATE,
    year                INT,
    quarter             INT,
    month               INT,
    day                 INT,
    day_of_week         INT,
    month_name          VARCHAR(20),
    dwh_create_date     DATETIME2       CONSTRAINT DF_DateKey_DWHCreateDate DEFAULT SYSDATETIME(),
);

IF OBJECT_ID('gold.dim_disputePP01_item','U') IS NOT NULL
DROP TABLE gold.dim_disputePP01_item;
GO
CREATE TABLE gold.dim_disputePP01_item
(
    item_key            INT             IDENTITY NOT NULL,
    item_code           VARCHAR(100)    NULL,
    item_name           VARCHAR(255)    NULL,
    item_description    VARCHAR(255)    NULL,
    item_type           VARCHAR(50)     NULL,
    dwh_create_date     DATETIME2       CONSTRAINT DF_DisputePP01Item_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_DisputePP01Item_ItemKey PRIMARY KEY(item_key)
);

IF OBJECT_ID('gold.dim_disputePP01_payer','U') IS NOT NULL
DROP TABLE gold.dim_disputePP01_payer;
GO
CREATE TABLE gold.dim_disputePP01_payer
(
    payer_key           INT             IDENTITY NOT NULL,
    account_id          VARCHAR(50)     NULL,
    email_address       VARCHAR(255)    NULL,
    given_name          VARCHAR(100)    NULL,
    surname             VARCHAR(100)    NULL,
    middle_name         VARCHAR(100)    NULL,
    alternate_full_name VARCHAR(200)    NULL,
    country_code        VARCHAR(10)     NULL,
    payer_status        VARCHAR(5)      NULL,
    address_status      VARCHAR(5)      NULL,
    dwh_create_date     DATETIME2       CONSTRAINT DF_DisputePP01Payer_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_DisputePP01Payer_PayerKey PRIMARY KEY(payer_key)
);
GO

IF OBJECT_ID('gold.dim_disputePP01_shipping','U') IS NOT NULL
DROP TABLE gold.dim_disputePP01_shipping;
GO
CREATE TABLE gold.dim_disputePP01_shipping
(
    shipping_key        INT             IDENTITY NOT NULL,
    name                VARCHAR(255)    NULL,
    line1               VARCHAR(255)    NULL,
    city                VARCHAR(100)    NULL,
    country_code        VARCHAR(10)     NULL,
    postal_code         VARCHAR(20)     NULL,
    dwh_create_date     DATETIME2       CONSTRAINT DF_DisputePP01Shipping_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_DisputePP01Shipping_PayerKey PRIMARY KEY(shipping_key)
);
GO

IF OBJECT_ID('gold.dim_disputePP02_dispute','U') IS NOT NULL
DROP TABLE gold.dim_disputePP02_dispute;
GO
CREATE TABLE gold.dim_disputePP02_dispute
(
    dispute_key                 INT             IDENTITY NOT NULL,
    dispute_id                  NVARCHAR(50)    NULL,
    reason                      NVARCHAR(255)   NULL,
    status                      NVARCHAR(100)   NULL,
    dispute_state               NVARCHAR(100)   NULL,
    dispute_life_cycle_stage    NVARCHAR(100)   NULL,
    dispute_channel             NVARCHAR(50)    NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputePP02Dispute_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_DisputePP02Dispute_DisputeKey PRIMARY KEY(dispute_key)
);
GO

IF OBJECT_ID('gold.dim_disputePP02_Seller','U') IS NOT NULL
DROP TABLE gold.dim_disputePP02_Seller;
GO
CREATE TABLE gold.dim_disputePP02_Seller
(
    seller_key                  INT             IDENTITY NOT NULL,
    seller_merchant_id          NVARCHAR(100)   NULL,
    seller_email                NVARCHAR(255)   NULL,
    seller_name                 NVARCHAR(255)   NULL,
    seller_protection_type      NVARCHAR(100)   NULL,
    seller_protection_eligible  BIT             NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputePP02Seller_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_DisputePP02Seller_SellerKey PRIMARY KEY(seller_key)
);
GO

IF OBJECT_ID('gold.fact_disputePP01_transaction','U') IS NOT NULL
DROP TABLE gold.fact_disputePP01_transaction;
GO
CREATE TABLE gold.fact_disputePP01_transaction
(
    transaction_key             INT             IDENTITY NOT NULL,
    transaction_id              VARCHAR(50)     NULL,
    date_key                    INT             NULL,
    payer_key                   INT             NULL,
    shipping_key                INT             NULL,
    transaction_event_code      VARCHAR(10)     NULL,
    transaction_status          VARCHAR(50)     NULL,
    transaction_currency        VARCHAR(10)     NULL,
    transaction_amount          DECIMAL(18,2)   NULL,
    ending_balance              DECIMAL(18,2)   NULL,
    available_balance           DECIMAL(18,2)   NULL,
    item_count                  INT             NULL,
    total_item_amount           DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_FactDisputeP001Transaction_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_FactDisputeP001Transaction_Transactionkey PRIMARY KEY(transaction_key)
);
GO

IF OBJECT_ID('gold.fact_disputePP02_dispute','U') IS NOT NULL
DROP TABLE gold.fact_disputePP02_dispute;
GO
CREATE TABLE gold.fact_disputePP02_dispute
(
    dispute_fact_key            INT             IDENTITY NOT NULL,
    dispute_key                 INT             NULL,
    date_key                    INT             NULL,
    seller_key                  INT             NULL,
    dispute_amount              DECIMAL(18,2)   NULL,
    transaction_count           INT             NULL,
    money_movement_amount       DECIMAL(18,2)   NULL,
    seller_protection_eligible  BIT             NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_FactDisputeP002Dispute_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_FactDisputeP002Dispute_DisputeFactKey PRIMARY KEY(dispute_fact_key)
);
GO
