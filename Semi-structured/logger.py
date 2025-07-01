import pandas as pd
from datetime import datetime
def log_table(source=None,destination=None,action=None,engine=None):
    if engine is None:
        return
    log_df=pd.DataFrame([{
        "source": source or "None",
        "destination": destination or "None",
        "action": action or "None",
        "timestamp": datetime.now()
    }])
    log_df.to_sql("etl_logs", con=engine, if_exists="append", index=False)
    print(f"Log: {source or 'None'} → {destination or 'None'} | {action or 'None'}")
