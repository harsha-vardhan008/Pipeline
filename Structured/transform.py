import pandas as pd 
def trans(df):
    df1=df
    # .explode("technologies")
    df1["_id"]=df1["_id"].astype(str)
    df1['start_date']=pd.to_datetime(df1["start_date"])
    df1['end_date']=pd.to_datetime(df1["end_date"])
    return df1
