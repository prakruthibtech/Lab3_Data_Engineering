FDE LAB 3 � ETL Pipeline Implementation: Exporting Data from SQL Server to PostgreSQL
Course: Foundations of Data Engineering (FDE)
Name: Prakruthi S
USN : 1RVU23CSE341
Objective
To design and implement an ETL (Extract, Transform, Load) pipeline that exports data from Microsoft SQL Server to PostgreSQL, ensuring secure connection, data transfer, and integrity between the two databases.
Outcomes
* Successfully established connection between SQL Server and PostgreSQL environments.
* Extracted relevant data from the AdventureWorks database hosted on SQL Server.
* Transferred and loaded the extracted data into the PostgreSQL AdventureWorks database.
* Created and configured an ETL role in PostgreSQL with appropriate privileges for smooth data migration and future integrations.
Materials Required
* etl.py � Python script implementing the ETL process
* Microsoft SQL Server Express and SQL Server Management Studio (SSMS)
* PostgreSQL and pgAdmin
* ODBC Driver for SQL Server
* AdventureWorks Sample Database
* Python Libraries: pyodbc, psycopg2, pandas, sqlalchemy
Lab Procedure
Stage 1: Environment Setup
Install SQL Server Express and SSMS, install PostgreSQL and pgAdmin, restore the AdventureWorks sample database, and verify that both database systems are operational.
Stage 2: Create Database and Role in PostgreSQL
Run the following SQL commands to create the PostgreSQL database and ETL role:
* CREATE DATABASE adventureworks WITH OWNER = postgres ENCODING = 'UTF8' LC_COLLATE = 'English_India.1252' LC_CTYPE = 'English_India.1252' LOCALE_PROVIDER = 'libc' TABLESPACE = pg_default CONNECTION LIMIT = -1 IS_TEMPLATE = False;
* CREATE ROLE etl WITH LOGIN PASSWORD 'demopass'; GRANT ALL PRIVILEGES ON DATABASE adventureworks TO etl;
* GRANT USAGE, CREATE ON SCHEMA public TO etl; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl;
Stage 3: Configure SQL Server for ETL Access
Create a login and user for ETL in SQL Server, grant connection permissions, and install the ODBC Driver for SQL Server.
Stage 4: Implement ETL Pipeline
The Python script (etl.py) connects to SQL Server, extracts data, transforms it, and loads it into PostgreSQL. Below is a sample code snippet:
import pyodbc
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# SQL Server Connection
sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=AdventureWorksDW2022;UID=etl;PWD=demopass')

# PostgreSQL Connection
pg_engine = create_engine('postgresql+psycopg2://etl:demopass@localhost/adventureworks')

# Extract Data
sales_data = pd.read_sql('SELECT TOP 100 * FROM FactInternetSales', sql_conn)

# Transform Data
sales_data = sales_data.fillna(0)

# Load Data
sales_data.to_sql('fact_internet_sales', pg_engine, if_exists='replace', index=False)

print('ETL process completed successfully!')
Stage 5: Verification
After running the ETL script, verify data consistency between SQL Server and PostgreSQL using COUNT queries on both databases.
Deliverables
File Name
Description
etl.py
Python script implementing ETL pipeline
AdventureWorksDW2022
SQL Server source database
adventureworks
PostgreSQL target database
ETL_Report.pdf
Final report with procedure, screenshots, and verification results
Conclusion
This experiment successfully demonstrates the end-to-end ETL pipeline for migrating data from Microsoft SQL Server to PostgreSQL. The pipeline ensures secure connectivity, reliable data extraction, transformation, and accurate loading into the PostgreSQL environment.
