import pandas as pd
from sqlalchemy import create_engine
import os


def extract():
    
    dir = r"C:\Users\Lyka Cedo\Desktop\Power BI\introduction-to-power-bi\Datasets\Contoso DW"
    try:
        for filename in os.listdir(dir):
            # get filename without extension
            file_wo_ext = os.path.splitext(filename)[0]
            # only process csv files
            if filename.endswith(".csv"):
                f = os.path.join(dir, filename)
                # checking if it is a file
                if os.path.isfile(f):
                    df = pd.read_csv(f)
                    # call load function
                    load(df, file_wo_ext)

    except Exception as e:
        print("Data error extraction of :", str(e))

def load(df, tbl_name):
    try:
        rows_imported = 0
        engine = create_engine("postgresql://etl:demopass@localhost:5432/db_demo")
        print(f"importing rows {rows_imported} to {rows_imported + len(df)}...")
        # save df to postgres
        df.to_sql(f"stg_{tbl_name}", engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f"data imported successfully")
    
    except Exception as e:
        print("Data load error: ", str(e))


if __name__ == "__main__":
    try:
        extract()
    
    except Exception as e:
        print("Error while extracting data: ", str(e))