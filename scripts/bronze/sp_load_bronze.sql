/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV and TXT files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Example Usage:
    EXEC bronze.sp_load_bronze;

NOTE:
	You have to manually provide the paths to your data files base on your local machines.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.sp_load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================================================';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading KAGGLE Tables';
		PRINT '-------------------------------------------------------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.kg_disease_description';
		TRUNCATE TABLE bronze.kg_disease_description;

		PRINT '>>> Inserting Data Into: bronze.kg_disease_description';
		BULK INSERT bronze.kg_disease_description
		FROM 'D:\CODE PROJECTS\__PTIT\PTIT AI\Disease_Diagnoses_Dwh\datasets\source_kaggle\disease_description.txt'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

        SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.kg_symptom_severity';
		TRUNCATE TABLE bronze.kg_symptom_severity;

		PRINT '>>> Inserting Data Into: bronze.kg_symptom_severity';
		BULK INSERT bronze.kg_symptom_severity
		FROM 'D:\CODE PROJECTS\__PTIT\PTIT AI\Disease_Diagnoses_Dwh\datasets\source_kaggle\symptom_severity.txt'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

        SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.kg_disease_diagnosis';
		TRUNCATE TABLE bronze.kg_disease_diagnosis;

		PRINT '>>> Inserting Data Into: bronze.kg_disease_diagnosis';
		BULK INSERT bronze.kg_disease_diagnosis
		FROM 'D:\CODE PROJECTS\__PTIT\PTIT AI\Disease_Diagnoses_Dwh\datasets\source_kaggle\disease_diagnosis.txt'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading CCMS Tables';
		PRINT '-------------------------------------------------------------------------------';
		PRINT '';
		
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.ccms_disease_category';
		TRUNCATE TABLE bronze.ccms_disease_category;

		PRINT '>>> Inserting Data Into: bronze.ccms_disease_category';
		BULK INSERT bronze.ccms_disease_category
		FROM 'D:\CODE PROJECTS\__PTIT\PTIT AI\Disease_Diagnoses_Dwh\datasets\source_ccms\disease_category.txt'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>> Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.ccms_disease_icd_10';
		TRUNCATE TABLE bronze.ccms_disease_icd_10;

		PRINT '>>> Inserting Data Into: bronze.ccms_disease_icd_10';
		BULK INSERT bronze.ccms_disease_icd_10
		FROM 'D:\CODE PROJECTS\__PTIT\PTIT AI\Disease_Diagnoses_Dwh\datasets\source_ccms\disease_icd_10.txt'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '\t',
			ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>> Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		SET @batch_end_time = GETDATE();
		PRINT '===============================================================================';
		PRINT 'Loading Bronze Layer Is Completed';
        PRINT 'Total Loading Duration: ' + CAST(DATEDIFF(ms, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '===============================================================================';
	END TRY
	
	BEGIN CATCH
		PRINT '===============================================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================================================';
	END CATCH
END
