/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
*/

USE [PaypalAnalytic];
GO

IF OBJECT_ID('dbo.etl_pipeline_audit', 'U') IS NOT NULL
    DROP TABLE dbo.etl_pipeline_audit;
GO

CREATE TABLE dbo.etl_pipeline_audit (
    audit_id            INT                 IDENTITY NOT NULL,
    run_id              UNIQUEIDENTIFIER    NOT NULL,
    layer_name          NVARCHAR(20)        NOT NULL,   -- Bronze / Silver / Gold
    step_name           NVARCHAR(100)       NOT NULL,  -- table / transform name
    source_system       NVARCHAR(50)        NOT NULL,
    target_object       NVARCHAR(100)       NOT NULL,
    start_time          DATETIME            NOT NULL,
    end_time            DATETIME            NOT NULL,
    duration_sec        INT                 NOT NULL,
    row_count           INT                 NOT NULL,
    status              NVARCHAR(20)        NOT NULL,   -- STARTED / SUCCESS / WARN / FAILED
    message             NVARCHAR(1000)      NOT NULL,
    created_at          DATETIME            CONSTRAINT DF_EtlPipelineAudit_CreatedAt DEFAULT GETDATE(),
    CONSTRAINT PK_EtlPipelineAudit_AuditId  PRIMARY KEY(audit_id),
    CONSTRAINT CK_EtlPipelineAudit_LayerName CHECK(layer_name IN ('Bronze', 'Silver', 'Gold')),
    CONSTRAINT CK_EtlPipelineAudit_SourceSystem CHECK(source_system IN ('ERP', 'CRM')),
    CONSTRAINT CK_EtlPipelineAudit_Status CHECK(status IN ('STARTED', 'SUCCESS', 'WARN', 'FAILED')),
    CONSTRAINT CK_EtlPipelineAudit_DurationSec CHECK(duration_sec >= 0),
    CONSTRAINT CK_EtlPipelineAudit_RowCount CHECK(row_count >= 0)
);
GO

IF OBJECT_ID('dbo.etl_error_log', 'U') IS NOT NULL
    DROP TABLE dbo.etl_error_log;
GO

CREATE TABLE dbo.etl_error_log (
    error_id            INT                 IDENTITY(1,1) NOT NULL,
    run_id              UNIQUEIDENTIFIER    NOT NULL,
    layer_name          NVARCHAR(20)        NOT NULL,   -- Bronze / Silver / Gold
    table_name          NVARCHAR(100)       NOT NULL,
    error_number        INT                 NOT NULL,
    error_severity      INT                 NOT NULL,
    error_state         INT                 NOT NULL,
    error_line          INT                 NOT NULL,
    error_procedure     NVARCHAR(200)       NOT NULL,
    error_message       NVARCHAR(4000)      NOT NULL,
    created_at          DATETIME            CONSTRAINT DF_EtlErrorLog_CreatedAt DEFAULT GETDATE(),
    CONSTRAINT PK_EtlErrorLog_ErrorId  PRIMARY KEY(error_id),
    CONSTRAINT CK_EtlErrorLog_LayerName CHECK(layer_name IN ('Bronze', 'Silver', 'Gold')),
);
GO
