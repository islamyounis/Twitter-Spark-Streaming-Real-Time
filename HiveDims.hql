
-- creating the external table top of the landing data with name ["Tweets_Staging"]
USE twitter_raw_data

CREATE EXTERNAL TABLE IF NOT EXISTS Tweets_Staging (
  tweet_id string, 
  text string, 
  created_at timestamp,
  like_count int,
  reply_count int,
  quote_count int,
  author_id string,
  author_username string,
  verified Boolean,
  author_followers_count int,
  author_following_count int,
  author_tweet_count int
)
PARTITIONED BY (year int, month int, day int, hour int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/home/case_study/twitter-landing-data/output'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'serialization.format'='1', 'field.delim'='\t')


MSCK REPAIR TABLE Tweets_Staging



-- ## creating dim tables ##

-- creating Author dimention with name ["Author_Dimention"] 

CREATE TABLE IF NOT EXISTS twitter_raw_data.Author_Dimention (
  author_id STRING,
  author_username STRING,
  verified Boolean,
  author_followers_count INT,
  author_following_count INT
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION '/home/case_study/twitter-raw-data/Author_Dimention'


MSCK REPAIR TABLE twitter_raw_data.Author_dimention


-- creating Tweet dimention with name ["Tweet_Dimention"] under under twitter_raw_data database

CREATE TABLE IF NOT EXISTS twitter_raw_data.Tweet_Dimention (
  tweet_id string,
  author_id STRING,
  text string,
  created_at timestamp,
  like_count int,
  reply_count int,
  quote_count int
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION '/home/case_study/twitter-raw-data/Tweet_Dimention'


MSCK REPAIR TABLE twitter_raw_data.Tweet_Dimention



-- ## inserting the data from staging table to the dimentions tables ##


-- #inserting the data into authoer dimention

hive.exec.dynamic.partition.mode nonstrict


    CREATE OR REPLACE TEMPORARY VIEW new_data_author_dim
    AS SELECT DISTINCT
        author_id,
        author_username,
        verified,
        author_followers_count,
        author_following_count,
        year,
        month,
        day,
        hour
    FROM Tweets_Staging


    INSERT INTO TABLE twitter_raw_data.Author_Dimention
    PARTITION (year, month, day, hour)
    SELECT
        n.author_id,
        n.author_username,
        n.verified,
        n.author_followers_count,
        n.author_following_count,
        n.year,
        n.month,
        n.day,
        n.hour
    FROM new_data_author_dim n
    LEFT JOIN twitter_raw_data.Author_Dimention a
    ON n.author_id = a.author_id
    WHERE a.author_id IS NULL or a.author_username IS NULL or a.author_followers_count IS NULL or a.author_following_count IS NULL



-- inserting the data into tweet dimention

hive.exec.dynamic.partition.mode nonstrict


    CREATE OR REPLACE TEMPORARY VIEW new_data_tweet_dim
    AS SELECT distinct
        tweet_id,
        author_id,
        text,
        created_at,
        like_count,
        reply_count,
        quote_count,
        year,
        month,
        day,
        hour
    FROM Tweets_Staging


    INSERT INTO TABLE Tweet_dimention
    PARTITION (year, month, day, hour)
    SELECT
        n.tweet_id,
        n.author_id,
        n.text,
        n.created_at,
        n.like_count,
        n.reply_count,
        n.quote_count,
        n.year,
        n.month,
        n.day,
        n.hour
    FROM new_data_tweet_dim n
    LEFT JOIN Tweet_dimention a
    ON n.tweet_id = a.tweet_id
    WHERE a.tweet_id IS NULL









