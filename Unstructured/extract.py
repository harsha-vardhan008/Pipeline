import configparser
import pandas as pd 
from  pymongo import MongoClient

def extract():
    config=configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\MangoDB\config.config")
    url=config['mongo']['url']
    client=MongoClient(url)
    db= client['First']
    collection=db["Unstructured"]
    doc=list(collection.find())
    df=pd.DataFrame(doc)
    return df

extract()
