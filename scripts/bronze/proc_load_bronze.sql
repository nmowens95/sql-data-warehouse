/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
============================================
*/

-- Check data available to Postgres
-- SHOW data_directory;

-- Procedure to truncate and load data into tables
CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql
AS $$
DECLARE
	rows_copied INT;
 	start_time TIMESTAMP; 
	end_time TIMESTAMP;
	duration INTERVAL;
BEGIN
	-- start timer
	start_time := clock_timestamp();
	RAISE NOTICE 'Process started at: %', start_time;

	-- crm_cust_info table
	RAISE NOTICE 'Loading crm_cust_info table...';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	BEGIN
		COPY bronze.crm_cust_info
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to crm_cust_info: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading crm_cust_info: %', SQLERRM;
	END;

	-- crm_prd_info table
	RAISE NOTICE 'Loading bronze.crm_prd_info table...';
	TRUNCATE TABLE bronze.crm_prd_info;
	
	BEGIN
		COPY bronze.crm_prd_info
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to crm_prd_info: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading crm_prd_info: %', SQLERRM;
	END;

	-- crm_sales_details table
	RAISE NOTICE 'Loading crm_sales_details...';
	TRUNCATE TABLE bronze.crm_sales_details;

	BEGIN
		COPY bronze.crm_sales_details
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to crm_sales_details: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading crm_sales_details: %', SQLERRM;
	END;
	
	-- erp_cust_az12 table
	RAISE NOTICE  'Loading erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	BEGIN
		COPY bronze.erp_cust_az12
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to erp_cust_az12: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading erp_cust_az12: %', SQLERRM;
	END;
	
	-- erp_loc_a101 table
	RAISE NOTICE 'Load erp_loc_a101...';
	TRUNCATE TABLE bronze.erp_loc_a101;

	BEGIN
		COPY bronze.erp_loc_a101
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to erp_loc_a101: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading erp_loc_a101: %', SQLERRM;
	END;

	-- erp_px_cat_g1v2 table
	RAISE NOTICE 'Loading erp_px_cat_g1v2...';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	BEGIN
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\Program Files\PostgreSQL\16\data\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		DELIMITER ','
		CSV HEADER;

		GET DIAGNOSTICS rows_copied = ROW_COUNT;
		RAISE NOTICE 'Rows copied to erp_px_cat_g1v2: %', rows_copied;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE WARNING 'Error loading erp_px_cat_g1v2: %', SQLERRM;
	END;
	
	-- end timer and total time
	end_time := clock_timestamp();
	duration := end_time - start_time;
	
	RAISE NOTICE 'Process completed at %', end_time;
	RAISE NOTICE 'Total execution time %', duration;
	
END;
$$;