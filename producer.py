import tweepy
from kafka import KafkaProducer
from json import dumps
from time import sleep
from datetime import datetime
import os
from dotenv import load_dotenv 

load_dotenv()

#twitter_keys
access_token = os.getenv("access_token") 
access_token_secret = os.getenv("access_token_secret") 
consumer_key = os.getenv("consumer_key") 
consumer_secret = os.getenv("consumer_secret") 
 

#twitter_authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x,indent=7).encode('utf-8'))

topic_name = "twitter"

def normalize_timestamp(time):
    t = str(time)
    my_time = datetime.strptime(t[:19], "%Y-%m-%d %H:%M:%S")
    return(my_time.strftime("%Y-%m-%d %H:%M:%S"))


res = api.search_tweets(q = ["Mac studio display"],until = '2022-03-20',lang = "en")
for i in res:
    data = {
            "name" : str(i.user.screen_name),
            "location": str(i.user.location),
            "datetime" : str(normalize_timestamp(i.created_at)),
            "followers": i.user.followers_count,
            "likes" : i.favorite_count,
            "retweets" : i.retweet_count,
            "tweet" : str(i.text),
            }
    producer.send(topic_name, value=data)
    sleep(5)    
