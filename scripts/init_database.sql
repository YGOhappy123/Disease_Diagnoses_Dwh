/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database named 'DiseaseDiagnosesDwh' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DiseaseDiagnosesDwh' database if it exists. 
    All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DiseaseDiagnosesDwh' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DiseaseDiagnosesDwh')
BEGIN
    ALTER DATABASE DiseaseDiagnosesDwh SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DiseaseDiagnosesDwh;
END;
GO

-- Create the 'DiseaseDiagnosesDwh' database
CREATE DATABASE DiseaseDiagnosesDwh;
GO

USE DiseaseDiagnosesDwh;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
