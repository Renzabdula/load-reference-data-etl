import pyodbc
import pandas as pd
from sqlalchemy import create_engine, URL
import os

# PostgreSQL connection settings 
user = os.environ['uid']
pwd = os.environ['pwd']
host = 'localhost'
port = 5432
dbname = 'db_demo'

# SQL Server connection settings
driver_name = '{SQL Server Native Client 11.0}'
server_name = 'LYKA\SQLEXPRESS'
db = 'db_demo'
uid = os.environ['uid']
pw = os.environ['pwd']




def extract():
    try:  
        # database connection
        engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{dbname}")
        print("Connected to database successfully") # debug information

        sql_query = """ SELECT table_name FROM information_schema.tables WHERE table_schema='public' """
        src_tables = pd.read_sql_query(sql_query, engine).to_dict()['table_name']
        print("Available tables:",src_tables) # debug information

        # load tables 
        for table_name in src_tables.values():
            print("Extracting data from:",table_name)
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', engine)
            print(df.info()) # debug information
            load(df, table_name)

    except Exception as e:
        print("Data extraction error: ", str(e))
    

def load(df, tbl):
    try:
        connection_string = f"Driver={driver_name};Server={server_name};Database={db};Trusted_Connection=yes;uid={uid};pwd={pw}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url, module=pyodbc)
        print("Connected to database successfully")

        rows_imported = 0
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)} for tables {tbl}') # debug information
        df.to_sql(f"{tbl}", engine, if_exists='replace', index=False)
        print("Data imported successfully") # debug information

    except Exception as e:
        print("Data load error: ", str(e))
    

if __name__=='__main__':
    extract()



