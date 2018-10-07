import sys
import json
import time
import datetime
import argparse
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import lit

#Read the command line arguments
parser = argparse.ArgumentParser(description='Spark Streaming')
parser.add_argument('--broker', type=str, default='ec2-18-235-25-194.compute-1.amazonaws.com:9092,ec2-52-202-106-225.compute-1.amazonaws.com:9092',
                    help="List of brokers which spark is listening to")
parser.add_argument('--topic', type=str, default='device-data',
                    help="name of the topic on which kafka is sending the data")
parser.add_argument('--gymLocations', type=str, default='/data/gymLocations.csv',
                    help="path where gym Location file exists on HDFS")
parser.add_argument('--userData', type=str, default='/data/userRecords.csv',
                    help="path where user records exist on HDFS")
args = parser.parse_args()

#Initialize the contexts#
sc = SparkContext(appName="HealthInsights")
ssc = StreamingContext(sc, 60) # 1 min window
sqlContext = SQLContext(sc)
EPSILON = 0.01


def findAvgHeartRatePerDevice(rdd):
    #Dont do anything if the rdd is NULL
    #if rdd.count() != 0:
    if rdd.isEmpty():
	print("RDD is empty")
    else:
        df = rdd.toDF()
        currTime = time.time();
        print("Schema of the dataframe:")
        df.printSchema()
	
	#Schema of the incoming data is:
	#root
 	#|-- deviceID: long (nullable = true)
 	#|-- Latitude: string (nullable = true)
	#|-- Longitude: string (nullable = true)
 	#|-- TimeStamp: double (nullable = true)
 	#|-- HeartRate: long (nullable = true)
        
        #Cast the columns to double for airthmatic operations on them
        df = df.withColumn("_2",col("_2").cast("double"))
        df = df.withColumn("_3",col("_3").cast("double"))
  
        #Name the columns
	df  = df.selectExpr("_1 as deviceID",\
			    "_2 as Longitude",\
			    "_3 as Latitude",\
			    "_4 as Time",\
			    "_5 as HeartRate ")
 
        #Filter out the out of order data as the data is time series. 
	#A late coming data point should not affect the results.
        df  = df.filter((df.Time - currTime)/60000 < 2)     

        #Average out the heart rate for each device id in the current window.
	groupedRDD = df.groupBy("deviceID")\
		       .agg(func.round(mean("Longitude"),4).alias("Longitude"),\
			    func.round(mean("Latitude"),4).alias("Latitude"),\
			    func.round(mean("HeartRate"),0).alias("avgHeartRate")
			   )
        
        #Schema of the groupedRDD dataframe is:
	#root
        #|-- DeviceID: long (nullable = true)
        #|-- Latitude: double (nullable = true)
        #|-- Longitude: double (nullable = true)
        #|-- AverageHeartRate: double (nullable = true)


	print("Schema of the grouped dataframe:")
        groupedRDD.printSchema()

        #Select deviceId, HeartRate and Timestamp from the data.
	selectedDF = df.select("deviceID","HeartRate","Time")

        #Join the existing records of user  with the incoming data
        userRecordsDF = userRecords.toDF()
        joinedDF = groupedRDD.join(userRecordsDF, "deviceID")
        print("joinedData")
      

        #Filter out those users who have not their heart rates in the desired 
	#range by comparing it with their existing health records
        filteredData = joinedDF.filter((joinedDF.avgHeartRate < joinedDF.minHeartRate) \
				       | (joinedDF.avgHeartRate > joinedDF.maxHeartRate))
        print("filteredData")
        print(filteredData.head(2))

        
        #Check if the location of the filtered users changed during the 2 minute window
	#Get the device id of the filtered users
	filteredDeviceRDD = filteredData.select("deviceID")
	print("HERE")
        print(filteredDeviceRDD.head(2))

        #Check if the patient is moving
	movingUsersRDD = filteredDeviceRDD.join(df,"deviceID")
        movingUsersRDD = movingUsersRDD.groupBy("deviceID")\
					.agg(func.countDistinct("Longitude","Latitude").alias("DistinctLocationsCount"))
      

        #Filter the moving users from the filteredRDD
        print("MOVING USERS")
	movingUsersRDD.printSchema() 
        stillUsersRDD = movingUsersRDD.filter(movingUsersRDD.DistinctLocationsCount == 1) 

 
        #Select all the heart rate values for the current window of the risked users
        displayRDD = stillUsersRDD.join(df,"deviceID")
        print("Count of filtered data: ",displayRDD.count())

        
        #Append the date to the s3 bucket folder in order to collect data for each in different folder
	#As this data would be pushed in the redshift at the end of the day, naming the tables based on
	#date would help in controlling the size of the table in the database
        now = datetime.datetime.now()
        date = now.strftime("%Y-%m-%d")
        time_of_the_day = now.strftime("%H-%M-%S")


        #Add time field to the aggregated data
        groupedRDD = groupedRDD.withColumn("Time", lit(now)) 
              
        #Save the aggregated data to S3
        if  len(groupedRDD.head(1)) != 0:
            groupedRDD.write \
                      .format("com.databricks.spark.csv") \
                      .mode("append") \
                      .save("s3n://anshu-insight/aggregatedData_"+ date + "/")
         

        #Save all the heart rate values for the risked users to S3 
        if len(displayRDD.head(1)) != 0:
            displayRDD.write \
                      .format("com.databricks.spark.csv") \
                      .mode("append") \
                      .save("s3n://anshu-insight/riskedUserData_" + date + "/")

def saveToDatabase(rdd):
    #if rdd.count() != 0:
    if rdd.isEmpty():
       print("Database:RDD is empty")
    else:
       df = rdd.toDF()
       now = datetime.datetime.now()
       date = now.strftime("%Y-%m-%d")
       #Save the raw data to S3. This data will be pushed to redShift dataware house once in a day.
       df.write \
         .format("com.databricks.spark.csv") \
         .mode("append") \
         .save("s3n://anshu-insight/rawData_" + date + "/")


def readFromKafkaAndProcess(topic,broker):
    inputFromKafka = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker})

    print("GOT HERE")
    
    #Extract the data from Kafka producer
    extractData = inputFromKafka.map(lambda x: json.loads(x[1]))

    extractData.pprint()
    
    #Map the data 
    mappedData = extractData.map(lambda x: (int(x["deviceId"]),(x["latitude"]),(x["longitude"]), x["time"],int(x['heartRate'])))
    

    #Process each incoming RDD
    mappedData.foreachRDD(findAvgHeartRatePerDevice)

    #Save the data
    mappedData.foreachRDD(saveToDatabase)
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":

    #Schema for the gym locations and user records data
    gymFileSchema = StructType([StructField("latitude", DoubleType()),StructField("longitude", DoubleType())])
    userDataSchema = StructType([StructField("deviceID", IntegerType()),StructField("minHeartRate", DoubleType()),StructField("maxHeartRate", DoubleType())])

    #Get the gym location data
    gymLocations = sqlContext.read \
                             .format("com.databricks.spark.csv") \
                             .schema(gymFileSchema) \
                             .option("header", "true") \
                             .option("mode", "DROPMALFORMED") \
                             .load(args.gymLocationFile)

    #Get the user records
    usersData = sqlContext.read \
                          .format("com.databricks.spark.csv") \
                          .schema(userDataSchema) \
                          .option("header", "true") \
                          .option("mode", "DROPMALFORMED") \
                          .load(args.userData)
 
    userRecords = usersData.rdd
    
    #Consume the data from Kafka and process it
    readFromKafkaAndProcess(args.topic,args.broker)


