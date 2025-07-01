import extract 
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import configparser
import urllib
from datetime import datetime


df=extract.read_data()
def sorted_orders(df):
# print(df.head())
    df_sort=df.sort_values(by ='order_id')
    return df_sort
def total(df):
    df_sort=df.copy()
    df_sort['sales']=df_sort['quantity']*df_sort['list_price']*(1-df_sort['discount'])
    df_sort['total_sales']=df_sort.groupby('customer_id')['sales'].transform('sum')
    #df_final=df_sort.merge(df_aggr,on='customer_id',how='inner')
    df_sort['categories']=pd.qcut(df_sort['total_sales'],q=3, labels=['Low', 'Medium', 'High'])
    return df_sort


dff=total(df)


#SCD 1
# print(filtered_df.head(50))
dff.to_csv('orders.csv',index=False)


def scdtype1(df, incremental_data):
    scd1_data = df.copy()
    for _, inc_row in incremental_data.iterrows():
        order_id = inc_row['order_id']
        new_shipped_date = inc_row['shipped_date']

        # Only proceed if shipped_date is not null
        if pd.notnull(new_shipped_date):
            # Check if order_id exists in df
            if order_id in scd1_data['order_id'].values:
                # Update the shipped_date and order_status = 4
                scd1_data.loc[scd1_data['order_id'] == order_id, 'shipped_date'] = new_shipped_date
                scd1_data.loc[scd1_data['order_id'] == order_id, 'order_status'] = 4

    return scd1_data
#SCD 2
def scdtype2(df_updated,df_new):
    df_final = pd.concat([df_updated,df_new],ignore_index=True)
    df_final = df_final.sort_values(by= 'order_id').reset_index(drop=True)
    return df_final
    

#SCD 4
def scd_type_4(df_new: pd.DataFrame, engine,
                     current_table,
                     history_table):
    try:
        df_current = pd.read_sql(f"SELECT * FROM {current_table}", engine)
    except Exception:
        print(f"Table {current_table} not found. Assuming first-time load.")
        df_current = pd.DataFrame()

    for _, new_row in df_new.iterrows():
        order_id = new_row['order_id']
        matched = df_current[df_current['order_id'] == order_id]

        if matched.empty:
            # Insert new record
            new_row.to_frame().T.to_sql(current_table, engine, if_exists='append', index=False)
            print(f"Inserted new order_id {order_id}")
        else:
            existing_row = matched.iloc[0]
            has_change = any(existing_row[col] != new_row[col] for col in df_new.columns)

            if has_change:
                print(f" Change detected for order_id {order_id}. Archiving old row.")

                # Archive existing row
                archived = existing_row.to_dict()
                archived['changed_date'] = datetime.now()
                archived['operation_type'] = 'UPDATE'
                pd.DataFrame([archived]).to_sql(history_table, engine, if_exists='append', index=False)

                # Update current row
                #from sqlalchemy import text  # make sure this import is at the top

                with engine.begin() as conn:
                    delete_stmt = text(f"DELETE FROM {current_table} WHERE order_id = :order_id")
                    conn.execute(delete_stmt, {"order_id": order_id})
            else:
                print(f" No change for order_id {order_id}. Skipping.")

    print("SCD Type 4 complete using table:", current_table)
