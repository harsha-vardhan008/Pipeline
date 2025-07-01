import configparser
import pandas as pd 
from  pymongo import MongoClient




def extract():
    config=configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\MangoDB\config.config")
    url=config['mongo']['url']
    client=MongoClient(url)
    db= client['First']
    collection1 = db["Project"]
    # collection2=db["Unstructured"]
    doc1=list(collection1.find())
    # doc2=list(collection2.find())
    df1=pd.DataFrame(doc1)
    return df1
    # df2=pd.DataFrame(doc2)

    # print(df1.head())
    # print(df2.head())

    # df1.explode("technologies")

