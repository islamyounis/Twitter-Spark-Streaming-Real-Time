import datetime
import pytz
import subprocess
import requests
import os
import json
import pandas as pd
import csv
import datetime
import dateutil.parser
import unicodedata
import time
import socket 

os.environ['TOKEN'] = ""

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "egypt lang:en"
now = datetime.datetime.utcnow()
end_time = (now - datetime.timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.000Z')
start_time = (now - datetime.timedelta(days=6)).replace(hour=23, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.000Z')

max_results = 20
next_token = None
url = create_url(keyword, start_time, end_time, max_results)

 with open("response.json", "w") as outfile:
     json.dump(json_response, outfile)

s = socket.socket()
host = "127.0.0.1"
port = 3333
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
print("Received request from: " + str(address)," connection created.")

while True: 
    json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
    
    for data in json_response['data']:
        tweet_id = data['id']
        text = data['text']
        created_at = dateutil.parser.parse(data['created_at'])
        like_count = data['public_metrics']['like_count']
        reply_count = data['public_metrics']['reply_count']
        quote_count = data['public_metrics']['quote_count']
        author_id = data['author_id']

        author = None
        for user in json_response['includes']['users']:
            if user['id'] == author_id:
                author = user
                break

        created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        payload = {
            'tweet_id': tweet_id,
            'text': text,
            'created_at': created_at_str,
            'like_count': like_count,
            'reply_count': reply_count,
            'quote_count': quote_count,
            'author_id': author_id,
            'author_username': author['username'],
            'verified': author['verified'],
            'author_followers_count': author['public_metrics']['followers_count'],
            'author_following_count': author['public_metrics']['following_count'],
            'author_tweet_count': author['public_metrics']['tweet_count']
        }

        payload_str = json.dumps(payload)
        print("Sending:", payload_str.encode('utf-8'))
        clientsocket.send((payload_str + '\n').encode('utf-8'))

    if 'next_token' in json_response['meta']:
        next_token = json_response['meta']['next_token']
        url = create_url(keyword, start_time, end_time, max_results)
    else:
        break
    time.sleep(10) 
        
clientsocket.close()



    