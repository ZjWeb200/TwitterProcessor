# TwitterProcessor
This project amis to use Kafka and Spark to do some fun big data processings with Twitter. Twitter API is used to get 1% of public tweets (which is already lots of data). We store these tweets in Kafka and use Spark to analyze the hashtags contained in these tweets. Our goal is to find out top-8 popular hashtags in real-time. The result is shown within Flask for quick visualization. Besides these, we also use Spark to analysis user feelings (positive or negative) based on their tweets. We will show this analysis with a Python plot as well. As usual, this project is run on the cloud. This time, I chose Google Cloud Platform.

# Big Picture
Before going into the details, we have a big picture diagram to illustrate the project structure.
![diagram](https://github.com/ZjWeb200/TwitterProcessor/blob/master/diagram.png)

# Details about the project
## The Twitter API
I applied a Twitter API account from this link [Twitter API](https://developer.twitter.com/en/apps)
I got my keys and tokens from there (they are required almost every step in the project...).
To fetch the tweets, I used the [statuses/filter](https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter) API. This API responds in JSON format, and the actual tweet contents is under the 'text' key.

## Something about Apache Kafka
I chose to use Kafka here as a broker between the Twitter stream and Spark. Kafka temporarily stores input stream data locally and then redirects them to the processing engines (in our project Spark).
I used Python packages kafka and tweepy to stream twitter data to Kafka. The code is pretty neat and straightforward.

## Positive/Negative word monitor with Kafka and Spark
I gathered certain amount of data in Kafka, and sent them to Spark. I'm trying to monitor the positive/negative feelings of the users based on their tweets. I used pyspark Python library to process data in Spark read from Kafka. After getting the tweets into Spark, I extracted all the positive/negative words from them. flatMap and reduceByKey are used during the data processing. Finally, I used matplotlib library to plot feelings of users. The codes are included in the Kafka folder. One of the final plots is shown below: <br/>
![feelings]()

## Real-time processor with Spark for popular Twitter hashtags
In this part, I wish to construct a real-time top popular Twitter hashtag visualization. 
Note, Kafka is not involved in this part. What I've done is reading tweets from Twitter API directly into Spark. I used the elegant requests python library to send http requests to Twitter API and got the JSON format response. After extracting the tweet content under the 'text' key, send it to Spark in a data stream manner. <br/>
On the Spark end, I processed data every 1 sec, i.e. 1 sec mini batch. In order to update hashtag word counts, I need to store the history and update it in real-time. Here, I chose to use checkpoint file and updateStateByKey. A very good doc can be found here: [Cumulative Calculations: updateStateByKey()](https://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/chapter1/total.html). To get the top popular hashtags, we need to sort all the hashtags based on their appearance counts. I did this by using pystark package's sql. The SQL "select" query is able to give us the top n (n = 8 in this project) hashtags. Finally, send these data to Flask to show the real-time results. Flask gets data from Spark and sends data to our browser. These two events happen in parallel and are accomplished by Flask routing. <br/>
To plot chart in html, I used [Chart.js](https://github.com/chartjs/Chart.js).

