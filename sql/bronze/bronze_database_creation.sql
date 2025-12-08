--create database--
IF DB_ID('DW_Football_Staging') IS NULL
BEGIN
    PRINT 'Creating database DW_Football_Staging...';
    CREATE DATABASE DW_Football_Staging;
END
ELSE
BEGIN
    PRINT 'Database DW_Football_Staging already exists.';
END
GO

USE DW_Football_Staging;
GO


--create shemas--
create schema bronze;
go 

create schema silver;
go

create schema gold;
go