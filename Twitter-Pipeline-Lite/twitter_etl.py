import tweepy as tw
import pandas as pd
import json
from datetime import datetime
import s3fs 
from twitter_key import CONSUMER_KEY,CONSUMER_SECRET,ACCESS_KEY,ACCESS_SECRET


def run_twitter_etl():
    # Twitter authentication
    auth = tw.OAuth1UserHandler(ACCESS_KEY,ACCESS_SECRET)
    auth.set_access_token(CONSUMER_KEY,CONSUMER_SECRET)

    # Creating api object
    api = tw.API(auth)

    tweets = api.user_timeline(screen_name = '@SismologicoMX',
                                count = 200,
                                include_rts = False,
                                tweet_mode = 'extended')


    cleaned_tweets = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        cleaned_tweets.append(refined_tweet)
    df = pd.DataFrame(cleaned_tweets)
    df.to_csv('s3://cesar-airflow-tweets-pipeline/SismologicoMX_twitter_data.csv')
   

