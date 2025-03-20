/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables if they already exist.
	Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.kg_disease', 'U') IS NOT NULL
    DROP TABLE silver.kg_disease;
GO

CREATE TABLE silver.kg_disease (
    disease_id          INT IDENTITY(1,1),
    disease             NVARCHAR(255),
    icd_code            NVARCHAR(10),
    category            NVARCHAR(10),
    description         NVARCHAR(MAX),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.kg_symptom', 'U') IS NOT NULL
    DROP TABLE silver.kg_symptom;
GO

CREATE TABLE silver.kg_symptom (
    symptom_id          INT IDENTITY(1,1),
    english_name        NVARCHAR(255),
    vietnamese_name     NVARCHAR(255),
    symptom_severity    INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.kg_diagnosis', 'U') IS NOT NULL
    DROP TABLE silver.kg_diagnosis;
GO

CREATE TABLE silver.kg_diagnosis (
    diagnosis_id        INT IDENTITY(1,1),
    disease_id          INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.kg_diagnosis_symptoms', 'U') IS NOT NULL
    DROP TABLE silver.kg_diagnosis_symptoms;
GO

CREATE TABLE silver.kg_diagnosis_symptoms (
    diagnosis_id        INT,
    symptom_id          INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.ccms_disease_category', 'U') IS NOT NULL
    DROP TABLE silver.ccms_disease_category;
GO

CREATE TABLE silver.ccms_disease_category (
    category_key        NVARCHAR(10),
    category_name       NVARCHAR(MAX),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.ccms_disease_icd_10', 'U') IS NOT NULL
    DROP TABLE silver.ccms_disease_icd_10;
GO

CREATE TABLE silver.ccms_disease_icd_10 (
    code                NVARCHAR(10),
    english_name        NVARCHAR(MAX),
    vietnamese_name     NVARCHAR(MAX),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);
GO