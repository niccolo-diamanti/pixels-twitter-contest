import tweepy
import requests
import time
import pandas as pd

# Set up Twitter API credentials
consumer_key = '<REPLACE>'
consumer_secret = '<REPLACE>'
access_token = '<REPLACE>'
access_token_secret = '<REPLACE>'

file = open('oldest_id.txt', 'r')
read = file.readlines()
oldest_id = 0

for line in read:
    oldest_id = int(line)

print('OLDEST_ID processed: ', oldest_id)

# Authenticate with Twitter API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Define the Twitter account and hashtag to monitor
search_param = 'to:pixels_online OR #wenpixel'

# Define the URL of the API endpoint
url = 'https://pixels-data.xyz/wen'

# 1500 requests/15-min window (app-auth) 100,000 requests/24-hour window (application level)
max_request = 1041

tweets = tweepy.Cursor(api.search_tweets, q=search_param,
                       tweet_mode='extended', count=100).items(100)
count = 1

columns = ['created_at', 'user', 'user_id', 'tweet', 'tweet_id']
data = []

for tweet in tweets:
    created_at = tweet.created_at
    user_id = tweet.user.id_str
    user_name = tweet.user.screen_name
    tweet_id = tweet.id_str
    tweet_text = tweet.full_text

    data.append([created_at, user_name, user_id, tweet_text, tweet_id])

    payload = {'user_id': user_id, 'tweet_id': tweet_id}
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print('Tweet processed successfully: ', tweet_id)
    else:
        print('Tweet processing failed', tweet_id)

df = pd.DataFrame(data, columns=columns)
df = df.sort_values(by=['tweet_id'], ascending=False)

print(df)

oldest_id = int(df['tweet_id'].iat[0])
print('OLDEST_ID: ', oldest_id)

while True:
    print('=========================================================================')
    tweets = tweepy.Cursor(api.search_tweets, q=search_param,
                           tweet_mode='extended', since_id=oldest_id+1, count=100).items(100)

    count = count+1
    size = len(list(tweets))
    print('COUNT: ', count)
    print('SIZE: ', size)

    if count > max_request-1 or size < 1:
        break

    for tweet in tweets:
        created_at = tweet.created_at
        user_id = tweet.user.id_str
        user_name = tweet.user.screen_name
        tweet_id = tweet.id_str
        tweet_text = tweet.full_text

        data.append([created_at, user_name, user_id, tweet_text, tweet_id])

        payload = {'user_id': user_id, 'tweet_id': tweet_id}
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print('Tweet processed successfully')
        else:
            print('Tweet processing failed')

    df = pd.DataFrame(data, columns=columns)
    df = df.sort_values(by=['tweet_id'], ascending=False)

    print(df)

    oldest_id = int(df['tweet_id'].iat[0])
    print('OLDEST_ID: ', oldest_id)

    print('N of tweets downloaded till now {}'.format(len(df)))

# Write the oldest_id to file
with open('oldest_id.txt', 'w') as file:
    file.write(str(oldest_id))
