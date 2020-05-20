import os, json, bz2
from glob import glob
from datetime import datetime

id_str = ''
total = 0
files = [f for f in glob("**/*.json.bz2", recursive=True)]
for f in files:
  if os.stat(f).st_size == 0:
    continue

  print(f, datetime.now(), total)
  for line in bz2.open(f):
    try:
      tweet = json.loads(line)
    except json.decoder.JSONDecodeError:
      continue

    if ('delete' in tweet) or (id_str == tweet['id_str']):
      continue

    id_str = tweet['id_str']
    total += 1

print(len(files), total)
