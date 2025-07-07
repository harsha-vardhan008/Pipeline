from twitter_extractor import extract_tweets
from text_cleaner import clean_text
from sentiment_analyzer import get_sentiment
import pandas as pd
import configparser
from sqlalchemy import create_engine
import urllib


df=extract_tweets("marketing",max_results=10)

df['cleaned_text']=df['text'].apply(clean_text)
df['sentiment']=df['cleaned_text'].apply(get_sentiment)

config = configparser.ConfigParser()
config.read('config.ini')

params = urllib.parse.quote_plus(
    f"DRIVER={config['sqlserver']['driver']};"
    f"SERVER={config['sqlserver']['server']};"
    f"DATABASE={config['sqlserver']['database']};"
     f"Trusted_Connection={config['sqlserver']['trusted_connection']}"
)

engine=create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
df.to_csv("twitter_sentiment_output.csv", index=False, encoding='utf-8')

df.to_sql('twitter_sentiment', engine, if_exists='append', index=False)
