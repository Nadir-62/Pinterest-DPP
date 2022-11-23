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
def get_db_row(item: Data):
    data = dict(item)
    api_producer = KafkaProducer(
        bootstrap_servers = "localhost:9092",
        client_id = "Pinterest API  data",
        value_serializer=lambda api_message: dumps(api_message).encode("ascii") 
    )
    for api_message in data:
        api_producer.send(topic="NadirsPinterestData", value=api_message)
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
            fd, path = tempfile.mkstemp()
            file_name = path [-11:]
            try:
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(str(message))    
            finally:
                self.s3.meta.client.upload_file(path, str(self.bucket_name), f"file {file_name}")
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
            fd, path = tempfile.mkstemp()
            file_name = path [-11:]
            try:
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(str(message))    
            finally:
                self.s3.meta.client.upload_file(path, str(self.bucket_name), f"file {file_name}")
                os.remove(path)

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
