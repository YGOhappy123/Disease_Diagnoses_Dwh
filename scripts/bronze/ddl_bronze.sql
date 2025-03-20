/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.kg_disease_description', 'U') IS NOT NULL
    DROP TABLE bronze.kg_disease_description;
GO

CREATE TABLE bronze.kg_disease_description (
    disease             NVARCHAR(255),
    icd_code            NVARCHAR(10),
    description         NVARCHAR(MAX)
);
GO

IF OBJECT_ID('bronze.kg_symptom_severity', 'U') IS NOT NULL
    DROP TABLE bronze.kg_symptom_severity;
GO

CREATE TABLE bronze.kg_symptom_severity (
    english_name        NVARCHAR(255),
    vietnamese_name     NVARCHAR(255),
    symptom_severity    INT
);
GO

IF OBJECT_ID('bronze.kg_disease_diagnosis', 'U') IS NOT NULL
    DROP TABLE bronze.kg_disease_diagnosis;
GO

CREATE TABLE bronze.kg_disease_diagnosis (
    disease             NVARCHAR(255),
    symptoms            NVARCHAR(MAX)
);
GO

IF OBJECT_ID('bronze.ccms_disease_category', 'U') IS NOT NULL
    DROP TABLE bronze.ccms_disease_category;
GO

CREATE TABLE bronze.ccms_disease_category (
    category_key        NVARCHAR(10),
    category_name       NVARCHAR(MAX)
);
GO

IF OBJECT_ID('bronze.ccms_disease_icd_10', 'U') IS NOT NULL
    DROP TABLE bronze.ccms_disease_icd_10;
GO

CREATE TABLE bronze.ccms_disease_icd_10 (
    code                NVARCHAR(10),
    english_name        NVARCHAR(MAX),
    vietnamese_name     NVARCHAR(MAX)
);
GO