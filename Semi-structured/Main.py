import pandas as pd
import Extract as e 
import Transform as t
import Load as l
import logger as lo
def main():
    print("Starting ETL...")
    data = e.extract()
    print("Data extracted")
    td = t.trans(data)
    print("Data transformed")
    l.load_to_sqlserver(td, "trans_structured")
    print("Data loaded to SQL Server")
    engine = l.load_to_sqlserver(td, "trans_structured")
    lo.log_table(
    source="MongoDB > Project",
    destination="SQL Server > trans_structured",
    action="ETL: Extract-Transform-Load",
    engine=engine
    )

main()