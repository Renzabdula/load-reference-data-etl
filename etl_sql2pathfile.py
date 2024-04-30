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

dir = r'C:\Users\Lyka Cedo\Desktop\Power BI'

def extract():
    connection_string = f"Driver={driver_name};Server={server_name};Database={db};Trusted_Connection=yes;uid={uid};pwd={pw}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url, module=pyodbc)
    print("Connected to database successfully")

    src_query = """SELECT table_name FROM information_schema.tables"""
    src_table = pd.read_sql_query(f'{src_query}', engine).to_dict()['table_name']
    print(src_table)

    for table_name in src_table.values():
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"',engine)
        print(df.info())
        print(df.shape)
        load(df, table_name, dir)


def load(df, tbl, dir):
    # create a file path
    f = os.path.join(dir, tbl)
    # load to CSV
    df.to_csv(f'{f}.csv', index=False)
    for file_name in os.listdir(dir):
        print(file_name)
        

extract()