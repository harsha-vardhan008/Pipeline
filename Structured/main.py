import pandas as pd
import extract as e 
import transform as t
import load as l

def main():
    print("Starting ETL...")
    data = e.extract()
    print("Data extracted")
    td = t.trans(data)
    print("Data transformed")
    l.load_to_sqlserver(td, "trans_structured")
    print("Data loaded to SQL Server")
    engine = l.load_to_sqlserver(td, "trans_structured")


main()