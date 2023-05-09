import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark= SparkSession.builder.appName("TwitterStream").enableHiveSupport().getOrCreate()
spark


schema = StructType([
    StructField("tweet_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("reply_count", IntegerType(), True),
    StructField("quote_count", IntegerType(), True),
    StructField("author_id", StringType(), True),
    StructField("verified", BooleanType(), True),
    StructField("author_username", StringType(), True),
    StructField("author_followers_count", IntegerType(), True),
    StructField("author_following_count", IntegerType(), True),
    StructField("author_tweet_count", IntegerType(), True)
])

# read the tweet data from socket
tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 3333) \
    .load() \
    .select(from_json(col("value"), schema).alias("tweet"))

tweet_df = tweet_df.selectExpr(
    "tweet.tweet_id",
    "tweet.text",
    "tweet.created_at",
    "tweet.like_count",
    "tweet.reply_count",
    "tweet.quote_count",
    "tweet.author_id",
    "tweet.author_username",
    "tweet.verified",
    "tweet.author_followers_count",
    "tweet.author_following_count",
    "tweet.author_tweet_count"
)


tweet_df = tweet_df.withColumn("year", year("created_at")) \
                   .withColumn("month", month("created_at")) \
                   .withColumn("day", dayofmonth("created_at")) \
                   .withColumn("hour", hour("created_at"))


queryt = tweet_df \
    .repartition(2) \
    .writeStream \
    .partitionBy("year", "month", "day", "hour") \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/home/case_study/pib/checkpoint") \
    .option("path", "/home/case_study/pib/output") \
    .start()

print("----- streaming is running -------")



