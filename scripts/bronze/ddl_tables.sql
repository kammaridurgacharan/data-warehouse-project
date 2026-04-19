/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables.

Source Systems:
    - CRM: crm_cust_info, crm_prd_info, crm_sales_details
    - ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
===============================================================================
*/

-- -----------------------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Source: CRM System — Customer Information
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

-- -----------------------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Source: CRM System — Product Information
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id              INT,
    prd_key             TEXT,
    prd_nm              TEXT,
    prd_cost            NUMERIC(10, 2),
    prd_line            TEXT,
    prd_start_dt        DATE,
    prd_end_dt          DATE
);

-- -----------------------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Source: CRM System — Sales Details
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num         TEXT,
    sls_prd_key         TEXT,
    sls_cust_id         INT,
    sls_order_dt        INT,
    sls_ship_dt         INT,
    sls_due_dt          INT,
    sls_sales           INT,
    sls_quantity         INT,
    sls_price           INT
);

-- -----------------------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Source: ERP System — Customer Demographics
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid                 TEXT,
    bdate               DATE,
    gen                 TEXT
);

-- -----------------------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Source: ERP System — Customer Location
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid                 TEXT,
    cntry               TEXT
);

-- -----------------------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Source: ERP System — Product Category
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id                  TEXT,
    cat                 TEXT,
    subcat              TEXT,
    maintenance         TEXT
);
