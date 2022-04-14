import re
from textblob import TextBlob
import pandas as pd

df = pd.read_csv("tweets.csv")

ptweets = []
ntweets = []
for tweet in df.tweet:
    analysis = TextBlob(str(tweet))
    if analysis.sentiment.polarity > 0:
        ptweets.append(tweet)
    elif analysis.sentiment.polarity < 0:
        ntweets.append(tweet)
    
        
        
no_of_tweets = 1104

print("Positive tweets percentage: {} %".format(100*len(ptweets)/no_of_tweets))
print("Negative tweets percentage: {} %".format(100*len(ntweets)/no_of_tweets))
print("Neutral tweets percentage: {} % \
    ".format(100*(no_of_tweets -(len( ntweets )+len( ptweets)))/no_of_tweets))
  
