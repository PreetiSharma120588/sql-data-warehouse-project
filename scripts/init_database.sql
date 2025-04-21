/*
==================================================================
Create database and schemas
===================================================================
Script Purpose:
  This scrip creates a new database named 'DtaaWarehouse' after checking if it already exista. If the database exists, 
  it is dropped and recreated. Additionally, the script sets up three schemas within three schemas within the database:
  'bronze', 'silver', 'gold'.

WARNING: Running this script will drop the entire 'DataWarehouse' database if it exits.
          All data in the database will be permanently deleted. 
          Proceed with caution and ensure you have backuos before running this script.

--Drop  and recreate the 'DataWarehouse' database
IF Exists (select 1 from sys.databases where name 'DataWarehouse')
Begin
  Alter Database DataWarehouse set single user with rollbact immediate;
  Drp database DataWarehouse;
end;



create schema bronze;

create schema silver;

create schema gold;
