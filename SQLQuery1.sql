
USE master;
GO
CREATE LOGIN etl WITH PASSWORD = 'demopass', DEFAULT_DATABASE = AdventureWorksDW2022;
GO

USE AdventureWorksDW2022;
GO

CREATE USER etl FOR LOGIN etl;
GO

GRANT CONNECT TO etl;
GO
