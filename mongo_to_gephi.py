from pymongo import MongoClient
import networkx as nx

client = MongoClient('localhost', 27017)
db = client.twitter
tweet_collection = db.tweets
tweet_cursor = tweet_collection.find()

hashtag_graph = nx.Graph() 
# undirected network, hashtags in a single tweet form connections to others
# weight is the number of such connections being formed from the entire data

mentioned_user_graph = nx.Graph() 
# undirected network, users mentioned in a single tweet form connections to others
# weight is the number of such connections being formed from the entire data

user_to_mention_graph = nx.DiGraph()
# directed network, each connection is from author to  one of the mentioned users in a single tweet
# weight is the number of such connections being formed from the entire data

i = 0        
for document in tweet_cursor:
    # create hashtag network
    try:
        if len(document['entities']['hashtags']) != 0:
            for hashtag1 in document['entities']['hashtags']:
                for hashtag2 in document['entities']['hashtags']:
                    if hashtag1 != hashtag2:
                        if hashtag_graph.has_edge(hashtag1, hashtag2):
                            hashtag_graph[hashtag1][hashtag2]['weight'] += 0.5
                        else:
                            hashtag_graph.add_edge(hashtag1, hashtag2, weight = 0.5)
    except:
        print('wrong in adding hashtags')
        print(document['entities']['hashtags'])
        continue

    # create mentioned user network
    try:
        if len(document['entities']['user_mentions']) != 0:
            for mentioned_user1 in document['entities']['user_mentions']:
                for mentioned_user2 in document['entities']['user_mentions']:
                    if mentioned_user1 != mentioned_user2:
                        if mentioned_user_graph.has_edge(mentioned_user1, mentioned_user2):
                            mentioned_user_graph[mentioned_user1][mentioned_user2]['weight'] += 0.5
                        else:
                            mentioned_user_graph.add_edge(mentioned_user1, mentioned_user2, weight = 0.5)
    except:
        print('wrong in adding mentioned users')
        print(document['entities']['user_mentions'])
        continue

    # create user to mentioned user network
    try:
        if len(document['entities']['user_mentions']) != 0:
            user = document['user']['screen_name']
            for mentioned_user in document['entities']['user_mentions']:
                if user_to_mention_graph.has_edge(user, mentioned_user):
                    user_to_mention_graph[user][mentioned_user]['weight'] += 1.0
                else:
                    user_to_mention_graph.add_edge(user, mentioned_user, weight = 1.0)
    except:
        print('wrong in adding users')
        print(user)
        print(document['entities']['user_mentions'])
        continue
    i += 1

nx.write_gexf(hashtag_graph, 'hashtag_graph.gexf')
nx.write_gexf(mentioned_user_graph, 'mentioned_user_graph.gexf')
nx.write_gexf(user_to_mention_graph, 'user_to_mention_graph.gexf')

print('processed %d tweets' % i)
print('number of nodes in hashtag network:', hashtag_graph.number_of_nodes())
print('number of edges in hashtag network:', hashtag_graph.number_of_edges())
print('number of nodes in mentioned user network:', mentioned_user_graph.number_of_nodes())
print('number of edges in mentioned user network:', mentioned_user_graph.number_of_edges())
print('number of nodes in user to mention network:', user_to_mention_graph.number_of_nodes())
print('number of edges in user to mention network:', user_to_mention_graph.number_of_edges())
