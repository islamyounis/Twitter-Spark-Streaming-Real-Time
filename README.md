# Twitter-Spark-Streaming-Real-Time
An Apache Spark streaming project that attempts to use a variety of technologies and frameworks, including Python, Spark, HDFS, Hive, and Tubule, to gather, process, and analyse Twitter data in real-time with dashboard insights.

## Project Description
This project processes tweets retrieved from the Twitter API based on specific keywords, throught the Real-Time Analytics Engine, Apache Spark streaming.
and stores them in the Hadoop Distributed File System and Hive tables, processes them, and then represents some extracted insights data in a real-time dashboard.
Overall, the project makes it possible to gather, process, and analyse Twitter data effectively. The resulting insights are then presented in a simple and approachable way.

## Architecture Overview

<p align="center">
 <img src="https://user-images.githubusercontent.com/83661639/236959563-5659516c-20d3-4ff3-a014-b12ed697da9c.jpg">
</p>



Five data pipelines are used in the project to gather and process real-time Twitter data. A Python Twitter listener is used to feed the data across a TCP socket before it is fed into the Spark processing engine. The transformed data is then saved in HDFS parquet format.
A Star Schema model with two Hive Dimension tables is then created using the stored data, after which it is read. These dimensions are examined and used to build Hive Fact Tables using a PySpark programme. The foundation for developing analytic insights and visualisations is provided by these Fact Tables.


## Project Workflow

#### how different project pipelines work

<p align="center">
  <img src="https://user-images.githubusercontent.com/46838441/236661208-7bf0dbd3-18cd-49a1-82da-37e25fa6c4cf.png">
</p>



**1- Data Source System:**

The project pipeline begins with the Data Source System. By entering pertinent keywords, the system queries Twitter's [APIv2](https://developer.twitter.com/en/docs/twitter-api/getting-started/make-your-first-request) to retrieve tweets, user information, and location data.

A Python listener script that gets the most recent tweets together with the author and location information runs this operation. Each tweet also includes media data and hashtags that are extracted. The script is programmed to run every five minutes, ensuring that the information gathered is current and pertinent.

After the data has been extracted, it is sent to any port that serves as a TCP socket to allow communication with the following step in the pipeline. By doing this, it is made sure that the data can be easily processed and analysed.

**2- Data Collection System:**

The task of gathering, processing, and storing Twitter data falls within the purview of the Data Collection System.

The data collector from the port, acting as a conduit between the Twitter API and the Hadoop Distributed File System (HDFS), is a long-running process that is involved in this step. The Python Twitter Listener sends the Twitter data to the Spark Streaming Job application in a JSON format through a TCP Socket.

The information is then cleaned and processed with PySpark before being parsed into a PySpark data frame with a clear schema. The year, month, day, and hour are retrieved from the *'created_at'* column, which represents the creation date of the tweet, and saved in HDFS in a parquet manner.

The Data Collection System continuously operates, maintaining the stream's functionality and collecting data from the port that was opened in the pipeline's previous stage. This guarantees that the data is efficiently saved and is available for additional processing and analysis.

**3- Landing Data Persistence:**

In the Landing Data Persistence step, a Hive table called "twitter_landing_data" is built on top of the directory called "twitter-landing-data" where the data parquet files are kept.

A logical representation of the stored data is provided by this Hive table, which functions as a metadata layer. It enables users to query the data using syntax similar to SQL, making it simple to retrieve and analyse data.

**4- Landing to Raw ETL:**

The relevant Hive dimension tables are created using HiveQL queries during the Landing to Raw ETL stage. These dimensions are taken from the landing data, more precisely from the database called "twitter_landing_data" that was built in the pipeline's previous step.

The "tweet_data_raw," "user_data_raw," "place_data_raw," and "media_data_raw" stages produce four new Hive dimensions that, respectively, include information on tweets, users, locations, and tweet attachments' media. The same four columns used in the previous step are used to partition all output dimensions, which are kept in an HDFS directory called "twitter_raw_data."

**5- Raw To Processed ETL:**

This stage involves reading the Hive dimensions into a SparkSQL application and applying various aggregates to produce Hive fact tables, which transforms the raw data from the previous stage into processed data. Business insights and analysis are based on the fact tables. 

The newly created Hive fact tables include "user_activity_processed," which contains majors that describe users' activities, "users_tweets_processed," which contains majors related to the tweet and its authors, and "totals_metrics_processed," which contains majors that calculate totals needed for various analysis processes. Finally, the "twitter-processed-data" HDFS directory is used to hold all of the output facts.

**Shell Script Coordinator:**

To manage the project pipelines, a Shell script coordinator is utilized. The coordinator contains two Shell scripts to handle the different pipeline processes.

The first Shell script runs once during machine startup and continues running in the background to ensure the streaming process is always active. It includes the Python Listener and Spark Streaming Job scripts.

The second Bash script comprises the Hive Dimensions script, the Hive Facts, and the Dashboard script. This script is added to the Linux Cron Jobs file and runs every five minutes, ensuring that the data in the HDFS, Hive tables, and Dashboards are up-to-date.

## Project Files:
- `Twitter_Listener.py`: A Python script with functions that use Twitter API2 to retrieve streaming data from Twitter. The data is in JSON format and is sent via a TCP socket to the next stage. The data comprises tweets, users, and media information, as well as a list of hashtags extracted from each tweet and a batch ID used to identify each batch of data transferred.

- `Spark_Streaming.py`: The spark job which responsible for collecting, cleaning, and processing data from the Twitter_Listener. The data is then loaded into the HDFS in a partitioned parquet format using Year, Month, Day, and Hour columns as partitions. This Spark job serves as an ETL engine between the listener and the HDFS. Lastly, the Spark job creates a Hive table containing all the raw parquet data.

- `HiveDims.hql`: A Hive script with queries used for creating and inserting data into the Hive Dimensions tables.

- `facts.py`: a PySpark script that uses Hive dimensions tables to create facts tables with meaningful metrics and generate business insights from the collected data. The script also contains the Bokeh code to create a real-time dashboard for visualizing the data.

- `Coordinator.sh`: A Shell script that automates the execution of all pipeline scripts. It ensures that the Python listener and the Spark Streaming Job are always running by sending them to the background. The HiveDims and Fact scripts are run every 5 minutes.

- `Cron_Scripts`: This directory contains Shell scripts that automate the entire pipeline using Linux Crontab.
