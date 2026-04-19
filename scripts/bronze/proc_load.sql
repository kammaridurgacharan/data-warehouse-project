/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from CSV files
    mounted inside the Docker container at /data/.

    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the COPY command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();

Prerequisites:
    - CSV files must be mounted at /data/source_crm/ and /data/source_erp/
      inside the Docker container (see docker-compose.yml volumes).
    - Bronze tables must already exist (see 02_create_bronze_tables.sql).

Notes:
    - PostgreSQL COPY requires the files to be accessible by the postgres
      server process (not the client). Since the files are mounted into the
      container, we use server-side COPY (not \copy).
    - COPY ... WITH (FORMAT csv, HEADER true) handles skipping the header row
      automatically, equivalent to BULK INSERT's FIRSTROW = 2.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time   TIMESTAMP;
    v_end_time     TIMESTAMP;
    v_batch_start  TIMESTAMP;
    v_batch_end    TIMESTAMP;
    v_row_count    BIGINT;
BEGIN
    v_batch_start := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- ==========================================================
    -- CRM Tables
    -- ==========================================================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- crm_cust_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/data/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- crm_prd_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/data/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- crm_sales_details
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/data/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- ==========================================================
    -- ERP Tables
    -- ==========================================================
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- erp_cust_az12
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/data/source_erp/cust_az12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- erp_loc_a101
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/data/source_erp/loc_a101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- erp_px_cat_g1v2
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/data/source_erp/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Rows Loaded: %', v_row_count;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INT;
    RAISE NOTICE '>> -------------';

    -- ==========================================================
    -- Summary
    -- ==========================================================
    v_batch_end := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end - v_batch_start))::INT;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error State  : %', SQLSTATE;
    RAISE NOTICE '==========================================';
END;
$$;
