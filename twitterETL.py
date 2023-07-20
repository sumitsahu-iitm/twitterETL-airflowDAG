
import pandas as pd 
from datetime import datetime
import json
import s3fs 
import tweepy

def fetchTweets_etl():
    
    apiKey = "1681875959018815489-oyy73fpuyksRQRx6NfEi8GVQ0sVxRe" 
    apiKeySecret = "JQuSldzzPGR1UNh4FJRBDt5QKvvIB47qYeg8pjs39SXid" 
    accessKey = "aIZ1Zy59eln8FRDsfuP4OJdzY"
    accessKeySecret = "E89G6zzcKjEjqf5MJrsORfe0jmdT4iAN9cAEHP51uITfJuP3bR"


    # Twitter authentication
    auth = tweepy.OAuthHandler(apiKey, apiKeySecret)   
    auth.set_access_token(accessKey, accessKeySecret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            count=100,
                            include_rts = False,
                            tweet_mode = 'extended'
                            )

    tweetList = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(tweetList)
    df.to_csv('tweetsdata.csv')
    
fetchTweets_etl()
