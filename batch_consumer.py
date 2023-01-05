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
        accessKeyID = "AKIA5L4Z425B5VVQJQEM"
        secretAccessKey = "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"
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
        

    