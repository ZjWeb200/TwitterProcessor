from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
import requests
import sys

conf = SparkConf()
conf.setAppName("TwitterStreamApplication")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1) # micro_batch, process data every 1 second
ssc.checkpoint("checkpoint_TwitterStreamApp")   # save word freq for future use
dataStream = ssc.socketTextStream("localhost",9090)

# sum up all counts for each word
def sumup_tags_counts(new_values, total_sum):
    return (total_sum or 0) + sum(new_values)

# get sql context for easy data extraction
def return_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


# send (tag, count) pair to Flask app for visualization
def stream_dataframe_to_flask(df):
    top_tags = [str(t.tag) for t in df.select("tag").collect()]
    tags_count = [p.counts for p in df.select("counts").collect()]
    url = 'http://0.0.0.0:5050/updateData'      # Note: we use routing to assign URL resource to functions defined in flask.
    request_data = {'words': str(top_tags), 'counts': str(tags_count)}
    response = requests.post(url, data=request_data)   # send the http request to Flask and get response status code


# use Spark rdd and save into sql table. Then use sql query to get the top-word
def process_rdd(time, rdd):
    print("------------- %s --------------" % str(time))
    try:
        sql_context_instance = return_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(tag=w[0], counts=w[1]))
        print(row_rdd)
        tags_counts_df = sql_context_instance.createDataFrame(row_rdd)
        tags_counts_df.registerTempTable("tag_with_counts")
        selected_tags_counts_df = sql_context_instance.sql("select tag, counts from tag_with_counts order by counts desc limit 8")
        selected_tags_counts_df.show()
        stream_dataframe_to_flask(selected_tags_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


words = dataStream.flatMap(lambda line: line.split(" ")) # flatten to 1D array of word strings
# only want words with hashtag. i.e. we generate an array of hashtag word tuples.
# tuple: x is the hashtag word, 1 means the count. 
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# reduce part of the MapReduce. merge values with the same key.
# read the checkpoint file word/count from disk, and send
# to sumup_tags_counts to aggregate the new counts.
tags_totals = hashtags.updateStateByKey(sumup_tags_counts)
tags_totals.foreachRDD(process_rdd)
ssc.start() # trigger the whole data pipeline
ssc.awaitTermination()  # keep Spark processing running unless we manually stop.






