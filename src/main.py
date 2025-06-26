import pandas as pd
import transformation as t
import extract as e
import load as l 
from sqlalchemy import text
from datetime import datetime
from transformation import scd_type_4 

#Function to create history table (if it doesn't exist)
def create_history_table_like_current(engine, current_table, history_table):
    create_sql = f"""
    if not exists (select * from sysobjects where name='{history_table}' and xtype='U')
    begin
        select *, 
            cast(null as datetime) as changed_date, 
            cast(null as varchar(10)) as operation_type
        into {history_table}
        from {current_table}
        where 1 = 0
    end
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        print(f" History table '{history_table}' created (if not existed) from '{current_table}'.")




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

    # SCD Type 4
      # Define table names
    current_table = "orders_transformed_ssms"
    history_table = "orders_transformed_ssms_history"
    df_new = e.read_new_orders()
    #  Get engine from load.py
    engine = l.get_sqlserver_engine()

    create_history_table_like_current(engine, current_table, history_table)

    scd_type_4(df_new=df_new, engine=engine,
               current_table=current_table,
               history_table=history_table)

    print(" SCD Type 4 process complete.")
    print(" Pipeline completed successfully.")
    # Load history table into MySQL (optional)
    history_df = pd.read_sql_table("orders_transformed_ssms_history", con=engine)
    mysql_engine = l.load_to_mysql(history_df, table_name="orders_transformed_history")



if __name__ == "__main__":
    main()
