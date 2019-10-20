from tweepy import OAuthHandler, Stream, StreamListener
from kafka import SimpleProducer, KafkaClient

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="YOUR CONSUMER KEY"
consumer_secret="YOUR CONSUMER SECRET"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="YOUR TOKEN" 
access_token_secret="YOUR TOKEN SECRET"

class KafkaListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    """
    # after get data from twitter
    def on_data(self, data):
        # send twitter stream data to Kafka.
        producer.send_messages("twitter-stream", data.encode("utf-8"))
        return True
    
    # if there is an error
    def on_error(self, status):
        print(status)

# Keep the file running to save data in Kafka for Spark's future use.
if __name__ == '__main__':

    kafka_client = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka_client)

    l = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['#'])

