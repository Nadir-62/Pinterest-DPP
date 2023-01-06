# Pinterest-DPP
Pinterest Data Processing Pipeline


## Milestone 2 - Briefing
- Pinterest Daily deadlines were not met as their previous legacy system could not process data fast enough

Their new pipeline has 3 main requirements:
- It had to be extencible enough so that new metrics could be added and back filled historically
- It had to be able to process a massive amount of data by a rapidly growing user base
- It has to be able to create dashboards using recent and historical data

Two processes are therefore run parallel to each other. The "Batch" and "Real Time" processing units.

Spark is 100 times faster than Hadoop as it executes commands in memory instead of reading and writing from a hard disk.

## Milestone 3 - Data Ingestion and Configuring the API

This task required for me to download the some Pinterest Infrastructure. This infrastructure included the porject API and the the user emulation programme.
Both programs were then run simultaneously.

## Milestone 4 -Data Ingestion - Consuming data in Kafka

Within this Milestone I was tasked with using Apache Kafka to create data piplines. 

The first task was to create a Kafka topic to send data to. This was done by first running zookeeper and kafka after which I used the : -

```
bin/kafka-topics.sh --create --topic NadirsPinterestData --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
to create my topic.

Next I had to create a Kafka Producer so that data from the API could be sent to the topic. This was done using kafka-python using the code below:
```
api_producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    client_id = "Pinterest API data",
    value_serializer=lambda api_message: dumps(api_message).encode("ascii") 
    )



    
@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
   
    api_producer.send(topic="MyPinterestData", value= data)
#This method is used to convert each message sent to the topic into bytes. 
#Kafka transports messages as bytes so we need to serialise our data into a format which is convertible to bytes
    
    return item

 ```

Finally I created two identical consumer pipelines; a batch processing pipeline and a streaming pipeline. At this point both pipelines were required to extract data from the kafka topic. This was done using the code below:
```
from kafka import KafkaConsumer
from json import loads

stream_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = "earliest"
)

stream_consumer.subscribe(topics = "NadirsPinterestData")

for message in stream_consumer:
    print(message)
    print (message.value)
    print(message.topic)
    print(message.timestamp)
```
## Milestone 5 - Batch Processing: Ingest data into the data lake
This Milestone required for me to send all the data from the topic into an S3 bucket via the batch processing pipeline.

The first thing I did was to create an S3 bucket by using boto3. I did this by creating the following method in my batch consumer python file:

```
    def s3_bucket(self):
        self.s3.create_bucket(Bucket=self.bucket_name,CreateBucketConfiguration={
         'LocationConstraint': 'eu-west-1',})
```

Next I had to find a way to send the data from the Kafka Consumer to the newly created bucket in the form of a JSON file. The method I used to do this was to create a temporary file within the message loop and then send that file to the bucket. The code below will show you what this looks like:

```
        for message in batch_consumer:
            consumer = tempfile.NamedTemporaryFile(mode ="w+")
            file_name = consumer.name [-11:]
            try:
                     json.dump(str(message),consumer)
                     consumer.flush()    
            finally:
                self.s3.meta.client.upload_file(consumer.name, str(self.bucket_name), f"file {file_name}.json")
                os.remove(path)
```
Here is my final code for the Batch consumer pipeline:
```
from kafka import KafkaConsumer
from json import loads
import boto3
import uuid
import json
import tempfile
import os

class Batch():
    
    def __init__(self):
        number = str(uuid.uuid4())[0:8]
        self.bucket_name = f"pinterest-data-{number}"
        self.s3 =boto3.resource("s3")
    
    def consumer(self):
        batch_consumer = KafkaConsumer(
            bootstrap_servers = "localhost:9092",
            value_deserializer = lambda message: loads(message),
            auto_offset_reset = "earliest"
        )

        batch_consumer.subscribe(topics = "NadirsPinterestData")

            
        for message in batch_consumer:
            consumer = tempfile.NamedTemporaryFile(mode ="w+")
            file_name = consumer.name [-11:]
            try:
                     json.dump(str(message),consumer)
                     consumer.flush()    
            finally:
                self.s3.meta.client.upload_file(consumer.name, str(self.bucket_name), f"file {file_name}.json")
                

    def s3_bucket(self):
        self.s3.create_bucket(Bucket=self.bucket_name,CreateBucketConfiguration={
         'LocationConstraint': 'eu-west-1',})
    
def Batch_consumer_pp():
    Pipeline = Batch()
    Pipeline.s3_bucket()
    Pipeline.consumer()
    
if __name__ == "__main__":
    Batch_consumer_pp()

```
## Milestone 6 - Batch Processing: Process the data using Spark
The first stage of this Milestone was to identify the appropriate methods to clean the data from the JSON files that were imported into the S3 bucket. The four ways in which the data needed to be cleaned or grouped were as followed:
1. Only the value column needed to be analysed
2. The dictionaries that were embedded within this column needed to be structured in a DataFrame, where the keys are the headers for their corresponding value.
3. After every iteration of the cleaning method for each file in the S3 bucket, the data will be joined to form a single DataFrame.
4. All errors or null rows will be appropriately replaced or removed.

The next stage was to ensure that Spark was able to access the S3 bucket. This was done by creating an OS environment whereby Apache spark could interact with the AWS S3 bucket:

```
 os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"
```
In order for this to be fulfilled a spark configuration had to be created along with the Spark session.
```
        cfg = SparkConf() \
            .setAppName("S3toSpark") \
        # Getting a single variable
        print(cfg.get("spark.executor.memory"))
        # Listing all of them in string readable format
        print(cfg.toDebugString())
        
        sc=SparkContext(conf=cfg)
        spark = SparkSession(sc).builder.appName("S3App").getOrCreate()
        accessKeyID = "A**********M"
        secretAccessKey = "u****************Z"
        hadoopConf=sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyID)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 
```
Now Spark can access any data from AWS S3.

The final stage is to implement the changes that were suggested in Stage 1. The code below shows how the transformation process was applied:
```

        result = spark.read.json(f"s3a://{self.bucket_name}/{self.bucket_list[0]}")
        for files in range(len(self.bucket_list)):
            try:    
                df_2 = spark.read.json(f"s3a://{self.bucket_name}/{self.bucket_list[files + 1]}")
                result = result.unionByName(df_2,True)
            except:
                result.drop("checksum","topic","headers", "key","serialized_key_size","serialized_header_size", "partition", "timestamp_type")
            
        value_column = result.select(col("value").cast("string").alias("value"))
        value_column.show(truncate=False)
        
        
        schema = StructType([
            StructField("category", StringType(),True),
            StructField("index", StringType(), True),
            StructField("unique_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("follower_count", StringType(), True),
            StructField("tag_list", StringType(), True),
            StructField("is_image_or_video", StringType(), True),
            StructField("image_src", StringType(), True),
            StructField("downloaded", StringType(), True),
            StructField("save_location", StringType(), True),
        ])
        
        df = value_column.withColumn("value",from_json(value_column.value,schema))
        df2 = df.select(col("value.*"))
        df2.withColumn("tag_list", regexp_replace("tag_list","N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" , "No Tags")) \
            .withColumn("follower_count", regexp_replace("follower_count","User Info Error" , "N/A"))\
            .withColumn("title", regexp_replace("title","No Title Data Available" , "N/A"))\
            .withColumn("description", regexp_replace("description","No description available Story format" , "N/A"))\
            .withColumn("image_src", regexp_replace("image_src","Image src error." , "N/A"))\
            .na.drop("all").show()
```

Here is a final update of what the Batch Consumer Pipeline looks like:

```
import sys
from kafka import KafkaConsumer
from json import loads
import multiprocessing
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce
import boto3
import uuid
import json
import tempfile
import os
import time

class Batch():
    
    def __init__(self):
        number = str(uuid.uuid4())[0:8]
        self.bucket_name = f"pinterest-data-{number}"
        self.s3 =boto3.resource("s3")
        self.client = boto3.client("s3")
        self.bucket_list = []
        self.values_list = []
    
    def consumer(self):
        batch_consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda message: loads(message),
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000
        )

        batch_consumer.subscribe(topics = "MyPinterestData")
        
        for message in batch_consumer:
            message_dict = {
                "topic" : message.topic,
                "partition" : message.partition,
                "offset" : message.offset,
                "timestamp" : message.timestamp,
                "timestamp_type" : message.timestamp_type,
                "key" : message.key,
                "value" : str(message.value),
                "headers" : message.headers,
                "checksum" : message.checksum,
                "serialized_key_size" : message.serialized_key_size,
                "serialized_value_size" : message.serialized_value_size,
                "serialized_header_size" : message.serialized_header_size
                }
            print(message.value)
            self.values_list.append(message.value)
            consumer = tempfile.NamedTemporaryFile(mode ="w+")
            file_name = consumer.name [-11:]
            json.dump(message_dict,consumer)
            consumer.flush()    
            self.s3.meta.client.upload_file(consumer.name, str(self.bucket_name), f"{file_name}.json")
            self.bucket_list.append(f"{file_name}.json")
        batch_consumer.close()

                

    def s3_bucket(self):
        self.s3.create_bucket(Bucket=self.bucket_name,CreateBucketConfiguration={
         'LocationConstraint': 'eu-west-1',})
        
    def spark_config(self):
        
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"
        
        cfg = SparkConf() \
            .setAppName("S3toSpark") \
        # Getting a single variable
        print(cfg.get("spark.executor.memory"))
        # Listing all of them in string readable format
        print(cfg.toDebugString())
        
        sc=SparkContext(conf=cfg)
        spark = SparkSession(sc).builder.appName("S3App").getOrCreate()
        accessKeyID = "A*************M"
        secretAccessKey = "********"
        hadoopConf=sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyID)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 
        

        result = spark.read.json(f"s3a://{self.bucket_name}/{self.bucket_list[0]}")
        for files in range(len(self.bucket_list)):
            try:    
                df_2 = spark.read.json(f"s3a://{self.bucket_name}/{self.bucket_list[files + 1]}")
                result = result.unionByName(df_2,True)
            except:
                result.drop("checksum","topic","headers", "key","serialized_key_size","serialized_header_size", "partition", "timestamp_type")
            
        value_column = result.select(col("value").cast("string").alias("value"))
        value_column.show(truncate=False)
        
        
        schema = StructType([
            StructField("category", StringType(),True),
            StructField("index", StringType(), True),
            StructField("unique_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("follower_count", StringType(), True),
            StructField("tag_list", StringType(), True),
            StructField("is_image_or_video", StringType(), True),
            StructField("image_src", StringType(), True),
            StructField("downloaded", StringType(), True),
            StructField("save_location", StringType(), True),
        ])
        
        df = value_column.withColumn("value",from_json(value_column.value,schema))
        df2 = df.select(col("value.*"))
        df2.withColumn("tag_list", regexp_replace("tag_list","N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" , "No Tags")) \
            .withColumn("follower_count", regexp_replace("follower_count","User Info Error" , "N/A"))\
            .withColumn("title", regexp_replace("title","No Title Data Available" , "N/A"))\
            .withColumn("description", regexp_replace("description","No description available Story format" , "N/A"))\
            .withColumn("image_src", regexp_replace("image_src","Image src error." , "N/A"))\
            .na.drop("all").show()
        
    def delete_bucket(self):
        bucket_name =  self.bucket_name
        Bucket = self.s3.Bucket(bucket_name)
        Bucket.objects.all().delete()
        
        self.client.delete_bucket(Bucket=bucket_name)
        print(f"{bucket_name} has been deleted")
            
            
    
def Batch_consumer_pp():
    Pipeline = Batch()
    Pipeline.s3_bucket()
    Pipeline.consumer()
    Pipeline.spark_config()
    Pipeline.delete_bucket()
    
if __name__ == "__main__":
    Batch_consumer_pp()
```
## Milestone 8 - Streaming: Kafka-Spark Intergration

Similar to the Batch consumer pipeline, I needed to intergrate Kafka and Spark so that the data that was being transmitted to the Kafka topic can then be extracted and transformed. This intergration was made possible by creating an environment with the Pyspark and Kafka intergration packages. A stream session was then created whereby the data will be read and loaded from the Kafka topic. The code below highlights how this was achieved:

```
    def stream_spark():     
        os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'   
        session = SparkSession \
            .builder \
            .appName("PinterestKafkaStreaming") \
            .getOrCreate()
        session.sparkContext.setLogLevel("Error")
        
        stream_df = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers","localhost:9092") \
            .option("subscribe", "MyPinterestData") \
            .option("startingOffsets", "earliest") \
            .load() 
``` 
## Milestone 9 - Spark Streaming: Transformation
In this Milestone I was required to apply transformations and computations to the streaming data. The computations I identified were the total sum of the followers in each micro-batch and the total number of followers per category. The cleaning transformation I carried was similar to the batch consumer pipeline, in that I created a structured dataframe for the value column of the kafka message. I then dropped all the null columns as well as replacing Error messages. Finally as we were doing arithmetic computations on the follower_count column I had to cast the column as an Integer. The code for this Milestone is shown below:

```
        
        stream_df = stream_df.selectExpr("CAST(value as STRING)")
        schema = StructType([
            StructField("category", StringType(),True),
            StructField("index", StringType(), True),
            StructField("unique_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("follower_count", StringType(), True),
            StructField("tag_list", StringType(), True),
            StructField("is_image_or_video", StringType(), True),
            StructField("image_src", StringType(), True),
            StructField("downloaded", StringType(), True),
            StructField("save_location", StringType(), True),
        ])
        df = stream_df.withColumn("value",from_json(stream_df.value,schema))
        df2 = df.select(col("value.*"))
        df3 = df2.withColumn("tag_list", regexp_replace("tag_list","N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" , "No Tags")) \
            .withColumn("follower_count", regexp_replace("follower_count","User Info Error" , "N/A"))\
            .withColumn("title", regexp_replace("title","No Title Data Available" , "N/A"))\
            .withColumn("description", regexp_replace("description","No description available Story format" , "N/A"))\
            .withColumn("image_src", regexp_replace("image_src","Image src error." , "N/A"))\
            .withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))\
            .na.drop("all")
        df3 = df3.withColumn("follower_count", df3["follower_count"].cast(IntegerType()))   
        
        def config_micro_batch(df, epoch_id):
            df.select(sum("follower_count")).show()
            df.groupBy("category").sum("follower_count").show()
            df.select(count(df.category)).show()
            df.printSchema()
 ```
 
## Milestone 10 - Spark Streaming: Load and Storage

This Milestone required me to load the Spark DataFrame onto a Postgresql database. This was firstly done by creating a local database and table with the commands below:

1) Run postgresql using this command
```
sudo -u postgres psql
```
2) Create the database 
```
CREATE DATABASE Pinterest_streaming;
```
3) Connect to the new database
```
\c Pinterest_streaming
```
4) create data table with the same columns as that of the Spark Dataframe
```
pinterest_streaming=# CREATE TABLE experimental_data(
category text,
index text,
unique_id text,
description text,
follower_count int,
tag_list text,
downloaded text,
save_location text,
title text,
is_image_src text,
is_image_or_video);
```
Now the Postgresql database is ready to connect to Spark.
The code below shows how this works:

```
        def config_micro_batch(df, epoch_id):
            df.select(sum("follower_count")).show()
            df.groupBy("category").sum("follower_count").show()
            df.select(count(df.category)).show()
            df.printSchema()
            
            df.write \
                .mode("append") \
                .format("jdbc") \
                .option("driver","org.postgresql.Driver") \
                .option("url", "jdbc:postgresql://localhost:5432/pinterest_streaming") \
                .option("dbtable", "experimental_data") \
                .option("user","postgres") \
                .option("password", "265762") \
                .save()
            pass
                            
        df3.writeStream \
            .foreachBatch(config_micro_batch) \
            .start() \
            .awaitTermination()
```
Here you can see that each micro-batch is appended to the SQL dataframe.
