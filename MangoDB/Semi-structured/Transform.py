import pandas as pd 
def trans(df1):
    dff1=df1.explode("technologies")
    dff1["_id"]=dff1["_id"].astype(str)
    dff1['start_date']=pd.to_datetime(dff1["start_date"])
    dff1['end_date']=pd.to_datetime(dff1["end_date"])
    return dff1

