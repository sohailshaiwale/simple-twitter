import tweepy
from kafka import KafkaProducer
from json import dumps
from time import sleep
from datetime import datetime

# #twitter_keys
access_token = "1491703690658336770-zmSabY3qRyNVHBNA6FlF4McZBLvo9q"
access_token_secret = "Gm6GyUf9KpI9ngUBVdf7XB2KySup4EAZSx82qRpbkmSt0"
consumer_key = "uYtNOr9tYvi2nFAJ3wMGiyk4m"
consumer_secret = "bfc9CZooksbwnbIYvj6XN1LwOgaxuyyp7whAWqkarLs3rfrUqX"

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