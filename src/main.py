import pandas as pd
import transformation as t
import extract as e
import load as l 

def main():
    # Step 1: Load Data
    print(" Starting ETL Pipeline...")

    #EXTRACT
   
    df = e.read_data()                        
    incremental = e.read_incremental_data()   
    new_orders = e.read_new_orders()          
    print(" Data extracted from sources.")

    #  Base transformation - calculate sales, categories
    final_df = t.total(df)
    print(" Base transformation complete.")

    # Apply SCD Type 1 (overwrite shipped date and order status)
    final_df = t.scdtype1(final_df, incremental)
    print(" SCD Type 1 updates applied.")

    # Apply SCD Type 2 (append new records)
    type2 = t.scdtype2(final_df, new_orders)
    print(" SCD Type 2 records generated.")

    # Load incremental data for tracking
    l.load_to_mysql(incremental, table_name='incremented2')
    print(" Incremental data loaded to MySQL.")

    #  Load final transformed data (post-SCD1)
    l.load_to_mysql(final_df, table_name="orders_transformed")
    l.load_to_sqlserver(final_df, table_name="orders_transformed_ssms")
    print("Final transformed data loaded to MySQL and SQL Server.")

    #  Load SCD Type 2 history data
    l.load_to_mysql(type2, table_name="scd2")
    l.load_to_sqlserver(type2, table_name="scd2")
    print("SCD Type 2 data loaded to MySQL and SQL Server.")

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()