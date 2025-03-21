/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)
    Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

IF OBJECT_ID('gold.dim_common_diseases', 'V') IS NOT NULL
    DROP VIEW gold.dim_common_diseases;
GO

CREATE VIEW gold.dim_common_diseases AS
SELECT
	ROW_NUMBER() OVER (ORDER BY disease_id)	AS N'Khóa bệnh',
	kd.icd_code								AS N'Mã bệnh theo ICD',
	COALESCE(
		di.english_name,	
		N'(Không có thông tin)'
	)										AS N'Tên bệnh quốc tế',
	COALESCE(
		di.vietnamese_name,	
		N'(Không có thông tin)'						
	)										AS N'Tên bệnh tiếng Việt',
	kd.description							AS N'Mô tả bệnh',
	kd.category								AS N'Mã phân loại',
	COALESCE(
		dc.category_name,
		di.vietnamese_name,
		N'(Không có thông tin)'
	)										AS N'Phân loại'
FROM silver.kg_disease AS kd
LEFT JOIN silver.ccms_disease_category dc ON kd.category = dc.category_key
LEFT JOIN silver.ccms_disease_icd_10 di ON kd.icd_code = di.code
GO

IF OBJECT_ID('gold.dim_common_symptoms', 'V') IS NOT NULL
    DROP VIEW gold.dim_common_symptoms;
GO

CREATE VIEW gold.dim_common_symptoms AS
SELECT
	ROW_NUMBER() OVER (ORDER BY symptom_id)	AS N'Khóa triệu chứng',
	st.english_name							AS N'Tên triệu chứng tiếng Anh',
	st.vietnamese_name						AS N'Tên triệu chứng tiếng Việt',
	st.symptom_severity						AS N'Mức độ nghiêm trọng'
FROM silver.kg_symptom AS st
GO

IF OBJECT_ID('gold.fact_likely_diseases_base_on_symptoms', 'V') IS NOT NULL
    DROP VIEW gold.fact_likely_diseases_base_on_symptoms;
GO

CREATE VIEW gold.fact_likely_diseases_base_on_symptoms AS
WITH symptom_occurrences AS (
    SELECT 
        symptom_id, 
        COUNT(ds.symptom_id) AS occurrence
    FROM silver.kg_diagnosis_symptoms AS ds
    GROUP BY ds.symptom_id
),
disease_diagnosis_cases AS (
	SELECT 
		ds.symptom_id,
		d.disease_id,
		COUNT(*) AS diagnosis_cases
	FROM silver.kg_diagnosis_symptoms AS ds
	INNER JOIN silver.kg_diagnosis AS d ON ds.diagnosis_id = d.diagnosis_id
	GROUP BY d.disease_id, ds.symptom_id
)
SELECT
	s.vietnamese_name										AS N'Tên triệu chứng',
	d.icd_code												AS N'Mã bệnh theo ICD',
	COALESCE(di.vietnamese_name, N'(Không có thông tin)')	AS N'Tên bệnh',
	so.occurrence											AS N'Số lần triệu chứng được ghi nhận',
	ddc.diagnosis_cases										AS N'Số lần bệnh được chẩn đoán',
	CAST(
		(100.0 * ddc.diagnosis_cases / so.occurrence)
		AS DECIMAL(10,2)
	)														AS N'Tỉ lệ % mắc bệnh khi có triệu chứng này'
FROM disease_diagnosis_cases ddc
INNER JOIN symptom_occurrences so ON ddc.symptom_id = so.symptom_id
INNER JOIN silver.kg_disease d ON ddc.disease_id = d.disease_id
INNER JOIN silver.kg_symptom s ON ddc.symptom_id = s.symptom_id
LEFT JOIN silver.ccms_disease_icd_10 di ON d.icd_code = di.code;
GO

IF OBJECT_ID('gold.fact_common_symptoms_of_diseases', 'V') IS NOT NULL
    DROP VIEW gold.fact_common_symptoms_of_diseases;
GO

CREATE VIEW gold.fact_common_symptoms_of_diseases AS
WITH disease_total_cases AS (
    SELECT 
        disease_id, 
        COUNT(DISTINCT d.diagnosis_id) AS total_cases
    FROM silver.kg_diagnosis AS d
    GROUP BY d.disease_id
),
symptom_diagnosis_occurrences AS (
	SELECT 
		d.disease_id,
		ds.symptom_id,
		COUNT(*) AS occurrence
	FROM silver.kg_diagnosis_symptoms AS ds
	INNER JOIN silver.kg_diagnosis AS d ON ds.diagnosis_id = d.diagnosis_id
	GROUP BY d.disease_id, ds.symptom_id
)
SELECT
	d.icd_code												AS N'Mã bệnh theo ICD',
	COALESCE(di.vietnamese_name, N'(Không có thông tin)')	AS N'Tên bệnh',
	s.vietnamese_name										AS N'Tên triệu chứng',
	dtc.total_cases											AS N'Số lần bệnh được chẩn đoán',
	sdo.occurrence											AS N'Số lần xuất hiện triệu chứng',
	CAST(
		(100.0 * sdo.occurrence / dtc.total_cases)
		AS DECIMAL(10,2)
	)														AS N'Tỉ lệ % xuất hiện triệu chứng'
FROM symptom_diagnosis_occurrences sdo
INNER JOIN disease_total_cases dtc ON sdo.disease_id = dtc.disease_id
INNER JOIN silver.kg_disease d ON sdo.disease_id = d.disease_id
INNER JOIN silver.kg_symptom s ON sdo.symptom_id = s.symptom_id
LEFT JOIN silver.ccms_disease_icd_10 di ON d.icd_code = di.code;
GO