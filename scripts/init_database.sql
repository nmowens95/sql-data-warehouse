/*
==========================================
Create Database and Schemas
==========================================

Script Purpose:
    Terminates session for 'DataWarehouse', and checks if 'DataWarehouse' exists. If it does
    exist the database 'DataWarehouse' is dropped, then the database 'DataWarehouse is created.
    It then creates the Medallion style architecture by creating Bronze, Silver and Gold schema layers.

WARNING:
    Running this script will drop the database 'DataWarehouse'. All data will be permanently deleted. 
    Make sure to have proper backups before running script.
*/

-- Drop and recreate the 'DataWarehouse' database

-- terminate connection to allow droping of database
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'DataWarehouse';

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

-- Connect to the new database (In psql, manually run: \c datawarehouse)

-- Create layers of warehouse
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;