import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark= SparkSession.builder.appName("TwitterStream").enableHiveSupport().getOrCreate()
spark

author_dim = spark.read.table("twitter_raw_data.Author_Dimention")
tweet_dim = spark.read.table("twitter_raw_data.Tweet_Dimention")


joined_df = author_dim.join(
    tweet_dim,
    (author_dim.author_id == tweet_dim.author_id) & 
    (author_dim.year == tweet_dim.year) & 
    (author_dim.month == tweet_dim.month) & 
    (author_dim.day == tweet_dim.day) & 
    (author_dim.hour == tweet_dim.hour)
)


country_words = [
    "Algeria", "Argentina", "Australia", "Austria","Brazil", "Canada", "China", "Colombia", "Denmark", "Ethiopia", "Finland", "France", "Germany",
    "Greece", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Italy", "Japan", "Kenya", "Mexico", "Netherlands",
    "New Zealand", "Nigeria", "Norway", "Pakistan", "Peru", "Philippines", "Poland", "Portugal", "Russia", "Saudi Arabia",
    "South Africa", "South Korea", "Spain", "Turkey", "Ukraine", "United Kingdom","United States", "Venezuela", "Vietnam",
     "Morocco", "Oman", "Palestine", "Qatar", "Saudi Arabia", "Somalia", "Sudan", "Syria", "Tunisia", "United Arab Emirates",
    "Yemen",  "Kuwait", "Lebanon", "Libya"
]


lowercase_country_words = list(map(str.lower, country_words))

df_tweet_country = tweet_dim .withColumn("text_lower", lower(col("text"))) \
    .withColumn("words", explode(split(col("text_lower"), " "))) \
    .filter(col("words").isin(*lowercase_country_words)) \
    .groupBy("year", "month", "day", "hour","tweet_id", "text") \
    .agg(collect_list("words").alias("countries")) \
    .withColumn("countries", array_distinct(col("countries"))) \
    .withColumn("num_countries", size(col("countries"))) \
    .withColumn("most_mentioned_country",explode("countries")).cache()


hashtag_count = size(split(tweet_dim.text, r'\s+#')) -1

window_spec = Window.partitionBy('year', 'month', 'day', 'hour')
df_tweet_country_select = df_tweet_country.groupBy("year", "month", "day", "hour","most_mentioned_country") \
    .agg(count(df_tweet_country.most_mentioned_country).alias("count")) \
    .withColumn('rank', rank()
    .over(window_spec.orderBy(col('count').desc()))) \
    .filter(col('rank') == 1) \
    .select('year', 'month', 'day', 'hour', 'most_mentioned_country').cache()



fact_table = author_dim.join(
    tweet_dim,
    (author_dim.author_id == tweet_dim.author_id) &
    (author_dim.year == tweet_dim.year) &
    (author_dim.month == tweet_dim.month) &
    (author_dim.day == tweet_dim.day) &
    (author_dim.hour == tweet_dim.hour)
).join(
    df_tweet_country,
    (tweet_dim.tweet_id == df_tweet_country.tweet_id),  
    "left"

).groupBy(
    tweet_dim.year,
    tweet_dim.month,
    tweet_dim.day,
    tweet_dim.hour
).agg(
 countDistinct(tweet_dim.tweet_id).alias("num_tweets"), 
    sum(when(author_dim.verified, 1).otherwise(0)).alias("num_verified"), 
    sum(length(tweet_dim.text)).alias("total_text_length"),  
    first(tweet_dim.tweet_id).alias("most_recent_tweet_id"), 
    countDistinct(tweet_dim.author_id).alias("num_authors"), 
    sum(hashtag_count).alias("num_hashtags"), 
    sum(when(df_tweet_country.num_countries.isNull(), 0).otherwise(df_tweet_country.num_countries)).alias("num_mentioned_country"), 
)


fact_table_m = fact_table.join(
    df_tweet_country_select,
    (fact_table.year == df_tweet_country_select.year) &
    (fact_table.month == df_tweet_country_select.month) &
    (fact_table.day == df_tweet_country_select.day) &
    (fact_table.hour == df_tweet_country_select.hour)
).cache()


# store the fact table in pass "/home/case_study/twitter-processed-data/fact_table"
fact_table_m = fact_table_m.coalesce(2)
fact_table_m.write.partitionBy("year", "month", "day", "hour").mode("overwrite").parquet("/home/case_study/twitter-processed-data/fact_table_1")



# show fact data
columns_to_keep = [fact_table.year, fact_table.month, fact_table.day, fact_table.hour, 'num_tweets', 'num_verified',
                   'total_text_length', 'most_recent_tweet_id', 'num_authors',
                   'num_hashtags', 'num_mentioned_country', 'most_mentioned_country']

fact_table_m.select(*columns_to_keep)
