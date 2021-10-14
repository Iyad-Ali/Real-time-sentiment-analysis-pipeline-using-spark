from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegressionModel




def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("tweet"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('tweet', F.regexp_replace('tweet', r'http\S+', ''))
    words = words.withColumn('tweet', F.regexp_replace('tweet', '@\w+', ''))
    words = words.withColumn('tweet', F.regexp_replace('tweet', '#', ''))
    words = words.withColumn('tweet', F.regexp_replace('tweet', 'RT', ''))
    words = words.withColumn('tweet', F.regexp_replace('tweet', ':', ''))

    tokenizer = Tokenizer(inputCol="tweet", outputCol="tweetTokens")
    tokenized_data = tokenizer.transform(words)

    swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="withoutStopWords")
    swr_data = swr.transform(tokenized_data)

    hash_transformer = HashingTF(inputCol=swr.getOutputCol(), outputCol="tweet_features")
    hashed_data = hash_transformer.transform(swr_data)

    preprocessed_data = hashed_data.select('tweet_features', 'tweet')

    return preprocessed_data



# text classification


def text_classification(words):
    # get predictions
    features =  words.selectExpr("tweet_features as features")
    predictions = model.transform(features)
    result = words.join(broadcast(predictions), words.tweet_features == predictions.features)

    return result.select("tweet", "prediction")


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder\
        .master("local")\
        .appName("SentimentAnalysis")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

    # read the tweet data from socket
    lines = spark.readStream.format("socket").option(
        "host", "0.0.0.0").option("port", 5555).load()

    # load model
    model = LogisticRegressionModel.load('/home/iyad-ali/Sentiment_Analysis/model/lr/')
    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words)

    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets") \
        .outputMode("append").format("parquet") \
        .option("path", "./parc") \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='60 seconds').start()
    query.awaitTermination()