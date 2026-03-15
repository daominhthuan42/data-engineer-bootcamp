/*
===============================================================================
DDL Script: Create Silver Tables for disputed_pp02
===============================================================================
*/

USE [PaypalAnalytic];
GO

IF OBJECT_ID('silver.disputed_pp02_disputes', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_disputes;
GO
CREATE TABLE silver.disputed_pp02_disputes (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    create_time                 DATETIME2       NULL,
    update_time                 DATETIME2       NULL,
    reason                      NVARCHAR(255)   NULL,
    status                      NVARCHAR(100)   NULL,
    dispute_state               NVARCHAR(100)   NULL,
    dispute_life_cycle_stage    NVARCHAR(100)   NULL,
    dispute_channel             NVARCHAR(50)    NULL,
    seller_response_due_date    DATETIME2       NULL,
    elton_created_at            DATETIME2       NULL,
    dt                          DATE            NULL,
    hour                        INT             NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_Disputes_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT PK_Dispute_DisputeId  PRIMARY KEY(dispute_id),
    CONSTRAINT CK_Dispute_Reason CHECK(reason IN ('CREDIT_NOT_PROCESSED', 'DUPLICATE_TRANSACTION', 
                                                  'INCORRECT_AMOUNT', 'MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED', 'MERCHANDISE_OR_SERVICE_NOT_RECEIVED', 
                                                  'OTHER', 'UNAUTHORISED')),
    CONSTRAINT CK_Dispute_Status CHECK(status IN ('RESOLVED', 'UNDER_REVIEW', 'WAITING_FOR_SELLER_RESPONSE')),
    CONSTRAINT CK_Dispute_DisputeState CHECK(dispute_state IN ('RESOLVED', 'REQUIRED_ACTION', 'UNDER_PAYPAL_REVIEW')),
    CONSTRAINT CK_Dispute_DisputeLifeCycleStage CHECK(dispute_life_cycle_stage IN ('CHARGEBACK', 'INQUIRY', 'PRE_ARBITRATION')),
    CONSTRAINT CK_Dispute_DisputeChannel CHECK(dispute_channel IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT CK_Dispute_Hour CHECK(hour >= 0)
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_amount', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_amount;
GO
CREATE TABLE silver.disputed_pp02_dispute_amount (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    currency_code               NVARCHAR(10)    NULL,
    amount_value                DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeAmount_DWHCreateDate DEFAULT SYSDATETIME(),
);
GO

IF OBJECT_ID('silver.disputed_pp02_disputed_transactions', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_disputed_transactions;
GO
CREATE TABLE silver.disputed_pp02_disputed_transactions (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    buyer_transaction_id        NVARCHAR(50)    NULL,
    seller_transaction_id       NVARCHAR(50)    NULL,
    create_time                 DATETIME2       NULL,
    transaction_status          NVARCHAR(50)    NULL,
    gross_amount_currency       NVARCHAR(10)    NULL,
    gross_amount_value          DECIMAL(18,2)   NULL,
    custom                      NVARCHAR(255)   NULL,
    buyer_name                  NVARCHAR(255)   NULL,
    seller_email                NVARCHAR(255)   NULL,
    seller_merchant_id          NVARCHAR(100)   NULL,
    seller_name                 NVARCHAR(255)   NULL,
    seller_protection_eligible  BIT             NULL,
    seller_protection_type      NVARCHAR(100)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputedTransactions_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeTransactions_SellerEmail CHECK(seller_email LIKE '%_@_%._%')
);
GO

IF OBJECT_ID('silver.disputed_pp02_disputed_transaction_items', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_disputed_transaction_items;
GO
CREATE TABLE silver.disputed_pp02_disputed_transaction_items (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    buyer_transaction_id        NVARCHAR(50)    NULL,
    item_name                   NVARCHAR(255)   NULL,
    item_description            NVARCHAR(255)   NULL,
    item_quantity               DECIMAL(18,3)   NULL,
    item_reason                 NVARCHAR(100)   NULL,
    item_item_type              NVARCHAR(50)    NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputedTransactionItems_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeTransactionItems_ItemReason CHECK(item_reason IN ('CREDIT_NOT_PROCESSED', 'DUPLICATE_TRANSACTION', 
                                                                           'INCORRECT_AMOUNT', 'MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED', 'MERCHANDISE_OR_SERVICE_NOT_RECEIVED', 
                                                                           'OTHER', 'UNAUTHORISED'))
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_adjudications', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_adjudications;
GO
CREATE TABLE silver.disputed_pp02_dispute_adjudications (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    adjudication_type           NVARCHAR(100)   NULL,
    adjudication_time           DATETIME2       NULL,
    reason                      NVARCHAR(255)   NULL,
    dispute_life_cycle_stage    NVARCHAR(100)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeAdjudications_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeAdjudications_DisputeLifeCycleStage CHECK(dispute_life_cycle_stage IN ('CHARGEBACK', 'INQUIRY', 'PRE_ARBITRATION'))
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_money_movements', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_money_movements;
GO
CREATE TABLE silver.disputed_pp02_dispute_money_movements (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    affected_party              NVARCHAR(50)    NULL,
    currency_code               NVARCHAR(10)    NULL,
    amount                      DECIMAL(18,2)   NULL,
    initiated_time              DATETIME2       NULL,
    movement_type               NVARCHAR(50)    NULL,
    reason                      NVARCHAR(255)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeMoneyMovements_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeMoneyMovements_Amount CHECK(amount >= 0)
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_evidences;
GO
CREATE TABLE silver.disputed_pp02_dispute_evidences (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    evidence_type               NVARCHAR(100)   NULL,
    notes                       NVARCHAR(MAX)   NULL,
    source                      NVARCHAR(100)   NULL,
    evidence_date               DATETIME2       NULL,
    dispute_life_cycle_stage    NVARCHAR(100)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeEvidences_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeEvidences_DisputeLifeCycleStage CHECK(dispute_life_cycle_stage IN ('CHARGEBACK', 'INQUIRY', 'PRE_ARBITRATION'))
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences_tracking', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_evidences_tracking;
GO
CREATE TABLE silver.disputed_pp02_dispute_evidences_tracking (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    carrier_name                NVARCHAR(100)   NULL,
    tracking_number             NVARCHAR(50)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeEvidencesTracking_DWHCreateDate DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_evidences_documents', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_evidences_documents;
GO
CREATE TABLE silver.disputed_pp02_dispute_evidences_documents (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    document_name               NVARCHAR(100)   NULL,
    document_url                NVARCHAR(MAX)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeEvidencesDocuments_DWHCreateDate DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_offer', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_offer;
GO
CREATE TABLE silver.disputed_pp02_dispute_offer (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    currency_code               NVARCHAR(10)    NULL,
    requested_amount            DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeOffer_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_DisputeOffer_RequestedAmount CHECK(requested_amount >= 0)
);
GO

IF OBJECT_ID('silver.disputed_pp02_refund_details', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_refund_details;
GO
CREATE TABLE silver.disputed_pp02_refund_details (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    currency_code               NVARCHAR(10)    NULL,
    allowed_refund_amount       DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_RefundDetails_DWHCreateDate DEFAULT SYSDATETIME(),
    CONSTRAINT CK_RefundDetails_AllowedRefundAmount CHECK(allowed_refund_amount >= 0)
);
GO

IF OBJECT_ID('silver.disputed_pp02_allowed_response_options_claim_types', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_allowed_response_options_claim_types;
GO
CREATE TABLE silver.disputed_pp02_allowed_response_options_claim_types (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    claim_type                  NVARCHAR(100)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_AllowedResponseOptionsClaimTypes_DWHCreateDate DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.disputed_pp02_allowed_response_options_offer_types', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_allowed_response_options_offer_types;
GO
CREATE TABLE silver.disputed_pp02_allowed_response_options_offer_types (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    offer_type                  NVARCHAR(100)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_AllowedResponseOptions_DWHCreateDate DEFAULT SYSDATETIME()
);
GO

IF OBJECT_ID('silver.disputed_pp02_dispute_extensions', 'U') IS NOT NULL
    DROP TABLE silver.disputed_pp02_dispute_extensions;
GO
CREATE TABLE silver.disputed_pp02_dispute_extensions (
    dispute_id                  NVARCHAR(50)    NOT NULL,
    merchant_contacted_outcome  NVARCHAR(100)   NULL,
    buyer_contacted_time        DATETIME2       NULL,
    correct_amount_currency     NVARCHAR(10)    NULL,
    correct_amount_value        DECIMAL(18,2)   NULL,
    dwh_create_date             DATETIME2       CONSTRAINT DF_DisputeExtensions_DWHCreateDate DEFAULT SYSDATETIME()
);
GO
