/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'silver' Tables.

Notes:
    - NVARCHAR(50)  -> TEXT (PostgreSQL has no NVARCHAR; TEXT is preferred)
    - DATETIME2     -> TIMESTAMPTZ
    - GETDATE()     -> NOW()
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Source: bronze.crm_cust_info (cleaned & deduplicated)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Source: bronze.crm_prd_info (cleaned, category extracted)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id              INT,
    cat_id              TEXT,
    prd_key             TEXT,
    prd_nm              TEXT,
    prd_cost            INT,
    prd_line            TEXT,
    prd_start_dt        DATE,
    prd_end_dt          DATE,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Source: bronze.crm_sales_details (dates cast, validated)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num         TEXT,
    sls_prd_key         TEXT,
    sls_cust_id         INT,
    sls_order_dt        DATE,
    sls_ship_dt         DATE,
    sls_due_dt          DATE,
    sls_sales           INT,
    sls_quantity        INT,
    sls_price           INT,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Source: bronze.erp_cust_az12 (gender standardized, dates validated)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid                 TEXT,
    bdate               DATE,
    gen                 TEXT,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Source: bronze.erp_loc_a101 (country standardized)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid                 TEXT,
    cntry               TEXT,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Source: bronze.erp_px_cat_g1v2
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id                  TEXT,
    cat                 TEXT,
    subcat              TEXT,
    maintenance         TEXT,
    dwh_create_date     TIMESTAMPTZ DEFAULT NOW()
);
