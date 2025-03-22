/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Example Usage:
    EXEC silver.sp_load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.sp_load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===============================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================================================================';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading KAGGLE Tables';
		PRINT '-------------------------------------------------------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.kg_disease';
		TRUNCATE TABLE silver.kg_disease;

		PRINT '>>> Inserting Data Into: silver.kg_disease';
		INSERT INTO silver.kg_disease
			(disease, icd_code, category, description)
		SELECT 
			TRIM(disease) AS disease,
			UPPER(TRIM(REPLACE(icd_code, '_', '.'))) AS icd_code,
			UPPER(LEFT(TRIM(icd_code), 3)) AS category,
			TRIM(REPLACE(description, CHAR(160), '')) AS description
		FROM bronze.kg_disease_description

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

        SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.kg_symptom';
		TRUNCATE TABLE silver.kg_symptom;

		PRINT '>>> Inserting Data Into: silver.kg_symptom';
		INSERT INTO silver.kg_symptom
			(english_name, vietnamese_name, symptom_severity)
		SELECT
			CONCAT(
				UPPER(LEFT(TRIM(english_name), 1)),
				LOWER(SUBSTRING(REPLACE(TRIM(english_name), '_', ' '), 2, LEN(TRIM(english_name)) - 1))
			) AS english_name,
			TRIM(REPLACE(vietnamese_name, CHAR(160), '')) AS vietnamese_name,
			CASE
				WHEN symptom_severity < 1 THEN 1
				ELSE symptom_severity
			END AS symptom_severity
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY LOWER(TRIM(english_name)) ORDER BY symptom_severity DESC
				) AS highest_flag
			FROM bronze.kg_symptom_severity
			WHERE symptom_severity IS NOT NULL
		) AS T
		WHERE highest_flag = 1;

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

        SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.kg_diagnosis and kg_diagnosis_symptoms';
		TRUNCATE TABLE silver.kg_diagnosis;
		TRUNCATE TABLE silver.kg_diagnosis_symptoms

		PRINT '>>> Inserting Data Into: silver.kg_diagnosis and kg_diagnosis_symptoms';
		SET NOCOUNT ON;
		DECLARE @affected_row_count INT = 0;
		DECLARE @disease NVARCHAR(255), @symptoms NVARCHAR(MAX);
		DECLARE diagnosis_cur CURSOR FOR SELECT disease, symptoms FROM bronze.kg_disease_diagnosis;

		OPEN diagnosis_cur;
		FETCH NEXT FROM diagnosis_cur INTO @disease, @symptoms;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			DECLARE @disease_id INT, @inserted_diagnosis_id INT;
			SELECT @disease_id = disease_id FROM silver.kg_disease WHERE disease = @disease;

			INSERT INTO silver.kg_diagnosis (disease_id) 
			VALUES (@disease_id);
			SELECT @inserted_diagnosis_id = @@IDENTITY;

			DECLARE @symptom NVARCHAR(255)
			DECLARE symptom_cur CURSOR FOR SELECT value FROM OPENJSON(@symptoms)

			OPEN symptom_cur
			FETCH NEXT FROM symptom_cur INTO @symptom
			WHILE @@FETCH_STATUS = 0
			BEGIN
				DECLARE @symptom_id INT;
				SELECT @symptom_id = symptom_id FROM silver.kg_symptom WHERE english_name = REPLACE(@symptom, '_', ' ');

				INSERT INTO silver.kg_diagnosis_symptoms (diagnosis_id, symptom_id)
				VALUES (@inserted_diagnosis_id, @symptom_id);

				FETCH NEXT FROM symptom_cur INTO @symptom
			END
			CLOSE symptom_cur
			DEALLOCATE symptom_cur

			SET @affected_row_count = @affected_row_count + 1;
			FETCH NEXT FROM diagnosis_cur INTO @disease, @symptoms;
		END
		CLOSE diagnosis_cur;
		DEALLOCATE diagnosis_cur;
		PRINT '';
		PRINT '(' + CAST(@affected_row_count AS VARCHAR) + ' rows affected)';
		SET NOCOUNT OFF;

		SET @end_time = GETDATE();
		PRINT '>>> Table Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading CCMS Tables';
		PRINT '-------------------------------------------------------------------------------';
		PRINT '';
		
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.ccms_disease_category';
		TRUNCATE TABLE silver.ccms_disease_category;

		PRINT '>>> Inserting Data Into: silver.ccms_disease_category';
		INSERT INTO silver.ccms_disease_category
			(category_key, category_name)
		SELECT
			UPPER(TRIM(category_key)) AS category_key,
			TRIM(REPLACE(category_name, CHAR(160), '')) AS category_name
		FROM bronze.ccms_disease_category

		SET @end_time = GETDATE();
		PRINT '>>> Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.ccms_disease_icd_10';
		TRUNCATE TABLE silver.ccms_disease_icd_10;

		PRINT '>>> Inserting Data Into: silver.ccms_disease_icd_10';
		INSERT INTO silver.ccms_disease_icd_10
			(code, english_name, vietnamese_name)
		SELECT
			CASE
				WHEN LEN(TRIM(code)) <= 3 THEN UPPER(TRIM(code))
				ELSE UPPER(CONCAT(LEFT(TRIM(code), 3), '.', SUBSTRING(TRIM(code), 4, LEN(TRIM(code)) - 3)))
			END AS code,
			TRIM(english_name) AS english_name,
			TRIM(REPLACE(vietnamese_name, CHAR(160), '')) AS vietnamese_name
		FROM bronze.ccms_disease_icd_10

		SET @end_time = GETDATE();
		PRINT '>>> Loading Duration: ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '>>> -------------';
		PRINT '';

		SET @batch_end_time = GETDATE();
		PRINT '===============================================================================';
		PRINT 'Loading Silver Layer Is Completed';
        PRINT 'Total Loading Duration: ' + CAST(DATEDIFF(ms, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' millisecond(s)';
		PRINT '===============================================================================';
	END TRY
	
	BEGIN CATCH
		PRINT '===============================================================================';
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================================================';
	END CATCH
END
