import configparser
import tweepy
import pandas as pd
import time

config = configparser.ConfigParser(interpolation=None)
config.read('config.ini')

bearer_token = config['twtr']['bearer_token']
client = tweepy.Client(bearer_token=bearer_token)

def extract_tweets(query, max_results=10):
    try:
        tweets_data = client.search_recent_tweets(
            query=query,
            tweet_fields=['created_at', 'lang'],
            max_results=max_results
        )
        tweets = [{
            'tweet_id': tweet.id,
            'text': tweet.text,
            'created_at': tweet.created_at,
            'lang': tweet.lang
        } for tweet in tweets_data.data]
        return pd.DataFrame(tweets)
    
    except tweepy.TooManyRequests:
        print("⚠️ Rate limit reached. Sleeping for 15 minutes...")
        time.sleep(15 * 60)
        return extract_tweets(query, max_results)
