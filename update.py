from pymongo import MongoClient, UpdateOne
from datetime import datetime
import tweepy

CONSUMER_KEY=''
CONSUMER_SECRET=''
OAUTH_TOKEN=''
OAUTH_TOKEN_SECRET=''
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
api = tweepy.API(auth, retry_count=3, retry_delay=1, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

connection = MongoClient('localhost', 27017)
db = connection.twitter
collection = db.tweets

docs = list(collection.find({}, {'id_str': 1}))
# docs = list(collection.find({'in_reply_to_status_id_str': {'$ne': None}}, {'in_reply_to_status_id_str': 1}))
print(len(docs))

requests = []
for i in range(0, len(docs), 100):
  print(i, len(requests), datetime.now())
  tweets = api.statuses_lookup([d['id_str'] for d in docs[i:i+100]], include_entities=False, trim_user=True)
  # tweets = api.statuses_lookup([d['in_reply_to_status_id_str'] for d in docs[i:i+100]], include_entities=False, trim_user=True)
  for tweet in tweets:
    requests.append(UpdateOne({'id_str': tweet.id_str}, {'$set': {'retweet_count': tweet.retweet_count, 'favorite_count': tweet.favorite_count}}))
    # requests.append(UpdateOne({'in_reply_to_status_id_str': tweet.id_str}, {'$set': {'in_reply_to_status_text': tweet.text}}))

  if i % 10000 == 0:
    res = collection.bulk_write(requests)
    print(res.bulk_api_result)
    requests.clear()

if len(requests) > 0:
  res = collection.bulk_write(requests)
  print(res.bulk_api_result)
