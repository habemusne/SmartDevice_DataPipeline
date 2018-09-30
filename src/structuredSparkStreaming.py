import sys
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Initialize the contexts        
sc = SparkContext(appName="Streaming")
spark = SparkSession \
            .builder \
                .appName("Streaming") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()


#Read from the streaming Kafka source
def readFromKafkaAndProcess(topic,broker):

#Desired format of the incoming data
    jsonSchema = StructType([ StructField("time", TimestampType(), False)\
                                , StructField("deviceID", StringType(),False)\
                                , StructField("latitude", FloatType(), False)\
                                , StructField("longitude", FloatType(), False)\
                                ,  StructField("heartRate", FloatType(), False)\
                             ])

    nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    jsonOptions = { "timestampFormat": nestTimestampFormat }

#Read structured stream from kafka and extract the value (actual data) from the stream
    streamData = spark.readStream\
        .format("kafka")\
        .option("zookeeper.connect","localhost:2181")\
        .option("kafka.bootstrap.servers","ec2-18-235-25-194.compute-1.amazonaws.com:9092")\
        .option("subscribe",topic)\
        .load()\
        .select(from_json(col("value").cast("string"), jsonSchema, jsonOptions).alias("parsed_value"))

    streamData.printSchema()


#Select all the fields from the parsed_value
    jsonData = streamData\
			.select("parsed_value.time","parsed_value.deviceID","parsed_value.latitude","parsed_value.longitude","parsed_value.heartRate")

    print("jsonFData",jsonData)

#Group the data by deviceID and find the average of the heartRates coming from each device id
    groupedData = jsonData\
			.withWatermark("time","1 minutes")\
			.groupBy(window(jsonData.time,"2 minutes","2 minutes"),"deviceID")\
			.agg(mean("heartRate"))


    query = groupedData \
                  .writeStream \
	  	  .format("console")\
                  .outputMode("complete")\
                  .start()
    
    print("GOT HERE")

#Sink the processed data to s3. The aggregated data can not be directly 
#sinked to S3. It requires ForeachWriter functionality which is not 
#currently fully supported for pyhton. 
    #query = groupedData \
     #           .writeStream \
#		.option("path","s3n://anshu-insight/structStream/")\
#		.option("checkpointLocation","/home/ubuntu/")\
#		.outputMode("update")\
 #               .start()


    query.awaitTermination()


def writeToRedshift(rdd):
    #for record in rdd:
    if(rdd.count() != 0):
        rdd = rdd.map(lambda x: x.split(",")).map(lambda x: [str(y) for y in x])
        df = rdd.toDF()

        df.write\
            .format("com.databricks.spark.redshift")\
            .option("url", "jdbc:redshift://redshift-cluster-1.cy68m2eaezzl.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Awsuser123") \
            .option("dbtable", "device_data") \
            .option("forward_spark_s3_credentials",True)\
            .option("tempdir", "s3n://anshu-insight/tmp") \
            .mode("error") \
            .save()



if __name__ == "__main__":

    #Get the required parameters from the passed arguments 
    broker, topic, gymLocationFile, userData = sys.argv[1:]


    #Consume the data from Kafka and process it
    readFromKafkaAndProcess(topic,broker)



