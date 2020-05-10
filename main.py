import os, re, json, bz2, emoji
from glob import glob
from textblob import TextBlob
from datetime import datetime
import pycountry_convert as pc

def convert_bytes(num):
  for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
    if num < 1024.0:
      return "%3.1f %s" % (num, x)
    num /= 1024.0

def file_size(file_path):
  if os.path.isfile(file_path):
    file_info = os.stat(file_path)
    return convert_bytes(file_info.st_size)

def decode(text):
  text = text.replace('\n', ' ')
  text = text.replace('&amp;', '&')
  text = text.replace('&gt;', '>')
  text = text.replace('&lt;', '<')
  text = text.strip()
  return text

def sentiment(polarity):
  if polarity > 0:
    return 'positive'
  elif polarity == 0:
    return 'neutral'
  else:
    return 'negative'

id_str = ''
files = [f for f in glob("**/*.json.bz2", recursive=True)]
for f in files:
  if os.stat(f).st_size == 0:
    continue

  print(f, datetime.now())
  tweets = []
  for line in bz2.open(f):
    try:
      tweet = json.loads(line)
    except json.decoder.JSONDecodeError:
      continue

    if ('delete' in tweet) or (tweet['place'] is None) or (id_str == tweet['id_str']):
      continue
    if 'quoted_status' in tweet:
      tweet['quoted_status_text'] = tweet['quoted_status']['text']
      # tweet['quoted_user_id_str'] = tweet['quoted_status']['user']['id_str']
      tweet['quoted_screen_name'] = tweet['quoted_status']['user']['screen_name']
      del tweet['quoted_status_id']
      del tweet['quoted_status']
      del tweet['quoted_status_permalink']
    if 'retweeted_status' in tweet:
      tweet['retweeted_status_id_str'] = tweet['retweeted_status']['id_str']
      tweet['retweeted_status_text'] = tweet['retweeted_status']['text']
      # tweet['retweeted_user_id_str'] = tweet['retweeted_status']['user']['id_str']
      tweet['retweeted_screen_name'] = tweet['retweeted_status']['user']['screen_name']
      del tweet['retweeted_status']
    if 'extended_tweet' in tweet:
      tweet['text'] = tweet['extended_tweet']['full_text']
      tweet['entities'] = tweet['extended_tweet']['entities']
      del tweet['extended_tweet']
    if 'extended_entities' in tweet:
      del tweet['extended_entities']
    if 'display_text_range' in tweet:
      del tweet['display_text_range']
    if 'withheld_in_countries' in tweet:
      del tweet['withheld_in_countries']
    if tweet['coordinates'] is not None:
      tweet['coordinates']['longitude'] = tweet['coordinates']['coordinates'][0]
      tweet['coordinates']['latitude'] = tweet['coordinates']['coordinates'][1]
      del tweet['coordinates']['type']
      del tweet['coordinates']['coordinates']
    if tweet['place'] is not None:
      try:
        tweet['place']['continent'] = pc.country_alpha2_to_continent_code(tweet['place']['country_code'])
      except KeyError:
        tweet['place']['continent'] = ''
      del tweet['place']['id']
      del tweet['place']['url']
      del tweet['place']['bounding_box']
      del tweet['place']['attributes']
      del tweet['place']['place_type']
    if tweet['in_reply_to_status_id_str'] is None:
      del tweet['in_reply_to_status_id_str']
      # del tweet['in_reply_to_user_id_str']
      del tweet['in_reply_to_screen_name']

    del tweet['id']
    del tweet['truncated']
    del tweet['in_reply_to_status_id']
    del tweet['in_reply_to_user_id']
    del tweet['in_reply_to_user_id_str']
    del tweet['geo']
    del tweet['contributors']
    # del tweet['entities']
    del tweet['favorited']
    del tweet['retweeted']
    del tweet['timestamp_ms']
    del tweet['is_quote_status']
    del tweet['quote_count']
    del tweet['reply_count']
    del tweet['filter_level']
    del tweet['user']['id']
    del tweet['user']['id_str']
    del tweet['user']['location']
    del tweet['user']['url']
    del tweet['user']['description']
    del tweet['user']['translator_type']
    del tweet['user']['utc_offset']
    del tweet['user']['time_zone']
    del tweet['user']['geo_enabled']
    del tweet['user']['lang']
    del tweet['user']['contributors_enabled']
    del tweet['user']['is_translator']
    del tweet['user']['profile_background_color']
    del tweet['user']['profile_background_image_url']
    del tweet['user']['profile_background_image_url_https']
    del tweet['user']['profile_background_tile']
    del tweet['user']['profile_link_color']
    del tweet['user']['profile_sidebar_border_color']
    del tweet['user']['profile_sidebar_fill_color']
    del tweet['user']['profile_text_color']
    del tweet['user']['profile_use_background_image']
    del tweet['user']['profile_image_url']
    del tweet['user']['profile_image_url_https']
    del tweet['user']['default_profile']
    del tweet['user']['default_profile_image']
    del tweet['user']['following']
    del tweet['user']['follow_request_sent']
    del tweet['user']['notifications']
    del tweet['user']['protected']
    del tweet['user']['created_at']
    if 'profile_banner_url' in tweet['user']:
      del tweet['user']['profile_banner_url']

    tweet['text'] = decode(tweet['text'])
    tweet['source'] = re.sub('<.*?>', '', tweet['source'])
    tweet['polarity'] = round(TextBlob(tweet['text']).sentiment.polarity, 1)
    tweet['sentiment'] = sentiment(tweet['polarity'])
    tweet['created_at'] = datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S")
    # tweet['user']['created_at'] = datetime.strptime(tweet['user']['created_at'], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S")
    tweet['entities']['hashtags'] = [t['text'].lower() for t in tweet['entities']['hashtags']]
    tweet['entities']['urls'] = [t['expanded_url'].split('?')[0] for t in tweet['entities']['urls']]
    tweet['entities']['user_mentions'] = [t['screen_name'] for t in tweet['entities']['user_mentions']]
    tweet['entities']['symbols'] = [t['text'].upper() for t in tweet['entities']['symbols']]
    if 'media' in tweet['entities']:
      tweet['entities']['media'] = [t['media_url_https'] for t in tweet['entities']['media']]
    else:
      tweet['entities']['media'] = []
    tweet['entities']['emojis'] = [emoji.demojize(c).replace(':', '') for c in tweet['text'] if c in emoji.UNICODE_EMOJI]

    id_str = tweet['id_str']
    tweets.append(tweet)

  # print(len(tweets))
  with open('data.json', 'a', encoding='utf8') as fp:
    for tweet in tweets:
      json.dump(tweet, fp, ensure_ascii=False)
      fp.write('\n')

print(file_size('data.json'))
