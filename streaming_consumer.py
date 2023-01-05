from json import loads
from kafka import KafkaConsumer
from multiprocessing import Process 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

class streaming():
    
    def kafka_consumer():
        stream_consumer = KafkaConsumer(
        bootstrap_servers = "localhost:9092",
        value_deserializer = lambda message: loads(message),
        auto_offset_reset = "earliest"
        )
        stream_consumer.subscribe(topics = "MyPinterestData")


    
    def stream_spark():     
        os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.1 pyspark-shell'   
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
        
    
def streaming_consumer():
    stream = streaming
    p1= Process(target =stream.kafka_consumer())
    p1.start()
    p2= Process(target = stream.stream_spark())
    p2.start()
if __name__ == "__main__":
    streaming_consumer()
        
    
