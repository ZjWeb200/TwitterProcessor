import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = "YOUR TOKEN"
ACCESS_SECRET = "YOUR TOKEN SECRET"
CONSUMER_KEY = "YOUR CONSUMER KEY"
CONSUMER_SECRET = "YOUR CONSUMER SECRET"
twitter_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

# send out the http request to Twitter
def streamTweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    stream_param = [('language', 'en'), ('locations', '-74,40,-73,41'), ('track', '#')]
    stream_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in stream_param])
    resp = requests.get(stream_url, auth=twitter_auth, stream=True)
    print(stream_url, resp)
    return resp


# process the http response streaming data. give it to Spark.
def twitter_to_spark(resp, tcp_conn):
    # will stay in this for loop unless we kill the program.
    # because resp is a data stream, we always have the next line to render.
    for line in resp.iter_lines():
        try:
            all_tweet = json.loads(line)
			# Our actual tweet content is under the 'text' key.
            tweet_pure_text = all_tweet['text'] + '\n'   # pyspark can't accept stream, add '\n'

            print("Tweet pure text is : " + tweet_pure_text)
            print ("------------------------------------------")
            tcp_conn.send(tweet_pure_text.encode('utf-8', errors='ignore'))
        except:
            e = sys.exc_info()[0]
            print("Error is: %s" % e)


conn = None
TCP_IP = "localhost"
TCP_PORT = 9090
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((TCP_IP, TCP_PORT))
sock.listen(1)
print("Twitter end server is waiting for TCP connection...")
# we wait for the connection to Spark to be established to move on.
conn, addr = sock.accept()
print("Spark process connected. streaming tweets to Spark process.")
resp = streamTweets()    # get data from Twitter
twitter_to_spark(resp,conn)  # send the data to Spark


