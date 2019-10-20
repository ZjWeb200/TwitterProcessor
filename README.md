# TwitterProcessor
This project amis to use Kafka and Spark to do some fun big data processings with Twitter. Twitter API is used to get 1% of public tweets (which is already lots of data). We store these tweets in Kafka and use Spark to analyze the hashtags contained in these tweets. Our goal is to find out top-8 popular hashtags in real-time. The result is shown within Flask for quick visualization. Besides these, we also use Spark to analysis user feelings (positive or negative) based on their tweets. We will show this analysis with a Python plot as well.

# Big Picture
Before going into the details, we have a big picture diagram to illustrate the project structure.
![diagram]()



# Something about Apache Kafka
I chose to use Kafka here as a broker between the Twitter stream and Spark. Kafka temporarily stores input stream data locally and then redirects them to the processing engines (in our project Spark) later.

