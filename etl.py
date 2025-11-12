# etl_sqlserver.py

# Import needed libraries
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# SQL Server connection details
driver = "{ODBC Driver 17 for SQL Server}"
server = r".\SQLEXPRESS"  # your instance
database = "AdventureWorksDW2022"

# Build connection string (Windows Authentication)
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

# Create SQLAlchemy engine
connection_url = URL.create(
    "mssql+pyodbc",
    query={"odbc_connect": connection_string}
)
engine = create_engine(connection_url)

# Extract sample data from SQL Server
def extract_tables():
    try:
        with engine.connect() as conn:
            query = """
            SELECT t.name AS table_name
            FROM sys.tables t
            WHERE t.name IN (
                'DimProduct',
                'DimProductSubcategory',
                'DimProductCategory',
                'DimSalesTerritory',
                'FactInternetSales'
            )
            """
            tables = pd.read_sql_query(query, conn)
            print("Tables found:", tables)
            return tables['table_name'].tolist()
    except Exception as e:
        print("Data extract error:", str(e))
        return []

# Load data into PostgreSQL (example stub)
def load_to_postgres(df, tbl):
    try:
        # Adjust credentials and server for your PostgreSQL
        pg_engine = create_engine("postgresql://etl:demopass@localhost:5432/adventureworks")

        print(f"Importing {len(df)} rows into staging table stg_{tbl}")
        df.to_sql(f'stg_{tbl}', pg_engine, if_exists='replace', index=False, chunksize=10000)
        print("Data imported successfully into PostgreSQL")
    except Exception as e:
        print("Data load error:", str(e))

# Run ETL
if __name__ == "__main__":
    tables = extract_tables()
    for tbl in tables:
        with engine.connect() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {tbl}", conn)
            load_to_postgres(df, tbl)
