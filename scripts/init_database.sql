/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.

USAGE:
    1. Connect to the 'postgres' (or any other maintenance DB) and run Part 1.
    2. Then reconnect to 'datawarehouse' and run Part 2.
       In psql:  \c datawarehouse
*/

-- ============================================
-- Part 1: Run while connected to 'postgres' DB
-- ============================================

-- Terminate all active connections to the database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();

-- Drop the database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the database
CREATE DATABASE datawarehouse;

-- ============================================
-- Part 2: Reconnect to 'datawarehouse' DB
--         psql:  \c datawarehouse
-- ============================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
