import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming.kafka import KafkaUtils

def load_word_list(word_list_file):
    words = set()
    f = open(word_list_file, "r")
    lines = f.read().split("\n")
    for line in lines:
        words.add(line)
        f.close()
    return words

def construct_plot(counts):
    pwords_counts = []
    nwords_counts = []

    for feeling_fields in counts:
        if feeling_fields:
            # val[0] negative field, val[1] positive field
            nwords_counts.append(feeling_fields[0][1])
            pwords_counts.append(feeling_fields[1][1])

    time = []
    for i in range(len(pwords_counts)):
        time.append(i)

    pos_line = plt.plot(time, pwords_counts, 'ro-', label='pfeelings words')
    neg_line = plt.plot(time, nwords_counts, 'ko-', label='nfeelings words')
    plt.axis([0, len(pwords_counts) - 1, 0, max(max(pwords_counts), max(nwords_counts))+40])
    plt.xlabel('time')
    plt.ylabel('count')
    plt.legend(loc = 'upper right')
    plt.savefig('feelingAnalysis.png')

def main():
    # load
    nfeeling_words = load_word_list("/home/xxx/spark/Dataset/nFeeling.txt")
    pfeeling_words = load_word_list("/home/xxx/spark/Dataset/pFeeling.txt")
    
    # Initialize spark streaming context
    conf = SparkConf().setAppName("TwitterStreamApplication")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)    # 1 sec mini batch
    ssc.checkpoint("checkpoint_TwitterStreamApplication")

    # Processing data from Kafka
    # twitter-stream is our Kafka topic; metadata.broker.list means address of the Kafka server.
    # since KafkaUtils is from pyspark package, kstream is acutally rdds. This makes data following all rdds. 
    kstream = KafkaUtils.createDirectStream(ssc, ["twitter-stream"], {"metadata.broker.list": "localhost:9092"})
    # default tweet format from Twitter API is JSON array with each tweet as a JSON string.
    # kstream (Kafka data format) is a list of (key, value) tuples. key: message metadata i.e. partition etc. value: message contents i.e. tweets.
    # we just want the value of kstream to analysis pos/neg feelings of the tweet, thus, x[1]. Only take English contents, ignore others.
    # map here means traverse all the kstream tuples and save what we want into "tweets" variable.
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))
    # for each line of tweet split them into list of strings.
    # then flatMap them into one large list of strings for all the tweets.
    # flatMap means 2d to 1d list.
    words = tweets.flatMap(lambda line:line.split(" "))
    nfeelings = words.map(lambda word: ("nfeelings", 1) if word in nfeeling_words else ("nfeelings", 0))
    pfeelings = words.map(lambda word: ("pfeelings", 1) if word in pfeeling_words else ("pfeelings", 0))
    # combine pos and neg feelings tuples into one large list
    both_feelings = pfeelings.union(nfeelings)
    # add up all the pos/neg feeling counts respectively.
    feeling_counts = both_feelings.reduceByKey(lambda x,y: x+y)
    
    counts = []
    # since feeling_counts is acutally rdd, use foreachRDD to add pos/neg tuples to counts list for plotting.
    # rdd.collect() action actually performs the reduceByKey transformation here. 
    feeling_counts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    ssc.start()
    ssc.awaitTerminationOrTimeout(10)
    ssc.stop(stopGraceFully = True)
    
    construct_plot(counts)
    
if __name__=="__main__":
    main()

