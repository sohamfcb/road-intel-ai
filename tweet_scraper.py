import tweepy
import pandas as pd
from dotenv import load_dotenv
import os
import json

def scrape_10_tweets(bearer_token:str):

    token_file = "next_token.json"
    next_token=None

    if os.path.exists(token_file):
        with open(token_file,'r') as f:
            data=json.load(f)
            next_token=data.get('next_token')

    client=tweepy.Client(bearer_token=bearer_token)
    query = "traffic jam OR road accident -is:retweet"

    if not os.path.exists("tweets.csv"):

        df=pd.DataFrame(columns=['text'])

        tweets = client.search_recent_tweets(query=query, max_results=10)
        text_data=[{'text':tweet.text} for tweet in tweets.data]
        df=pd.concat([df,pd.DataFrame(text_data)],ignore_index=True).drop_duplicates(subset=['text'], keep='first')

        df.to_csv("tweets.csv", index=False)

    else:

        df=pd.read_csv('tweets.csv')
        tweets = client.search_recent_tweets(query=query, max_results=10)

        text_data=[{'text':tweet.text} for tweet in tweets.data]

        df=pd.concat([df,pd.DataFrame(text_data)],ignore_index=True).drop_duplicates(subset=['text'], keep='first')

        df.to_csv("tweets.csv", mode='a', header=False, index=False)

    if tweets.meta.get("next_token"):
        with open(token_file,'w') as f:
            json.dump({'next_token':tweets.meta.get("next_token")},f)

if __name__=="__main__":

    load_dotenv()

    bearer_token = os.getenv("X_BEARER_TOKEN")
    scrape_10_tweets(bearer_token=bearer_token)