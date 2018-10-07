import sys
import json
import time
import datetime
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import lit

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
	df  = df.selectExpr("_1 as deviceID", "_2 as Longitude","_3 as Latitude","_4 as Time","_5 as HeartRate ")
 
        #Filter out the out of order data as the data is time series. A late coming data point should not affect the results.
        df  = df.filter((df.Time - currTime)/60000 < 2)     

        #Average out the heart rate for each device id in the current window.
	groupedRDD = df.groupBy("deviceID").agg(func.round(mean("Longitude"),4).alias("Longitude"),func.round(mean("Latitude"),4).alias("Latitude"),func.round(mean("HeartRate"),0).alias("avgHeartRate"))
        
        #Schema of the groupedRDD dataframe is:
	#root
        #|-- DeviceID: long (nullable = true)
        #|-- Latitude: double (nullable = true)
        #|-- Longitude: double (nullable = true)
        #|-- AverageHeartRate: double (nullable = true)


	print("Schema of the grouped dataframe:")
        groupedRDD.printSchema()
        #groupedRDD = groupedRDD.selectExpr("_1 as deviceID", "Longitude as Longitude","Latitude as Latitude","avgHeartRate as avgHeartRate")

        #Select deviceId, HeartRate and Timestamp from the data.
	selectedDF = df.select("deviceID","HeartRate","Time")

        #Join the existing records of user  with the incoming data
        userRecordsDF = userRecords.toDF()
        joinedDF = groupedRDD.join(userRecordsDF, "deviceID")
        print("joinedData")
        #print(joinedDF.head(2))

        #Filter out those users who have not their heart rates in the desired range by comparing it with their existing health records
        filteredData = joinedDF.filter((joinedDF.avgHeartRate < joinedDF.minHeartRate) | (joinedDF.avgHeartRate > joinedDF.maxHeartRate))
        print("filteredData")
        #print(filteredData.head(2))

        
        #Check if the location of the filtered users changed during the 2 minute window
	#Get the device id of the filtered users
	filteredDeviceRDD = filteredData.select("deviceID")
	print("HERE HERE")
        print(filteredDeviceRDD)

        #Name the columns
	#df  = df.selectExpr("_1 as deviceID", "_2 as Longitude","_3 as Latitude","_4 as Time","_5 as HeartRate ")
        
	movingUsersRDD = filteredDeviceRDD.join(df,"deviceID")
        movingUsersRDD = movingUsersRDD.groupBy("deviceID").agg(func.countDistinct("Longitude","Latitude").alias("DistinctLocationsCount"))
      

        #Filter the moving users from the filteredRDD
        print("MOVING USERS")
	movingUsersRDD.printSchema() 
        stillUsersRDD = movingUsersRDD.filter(movingUsersRDD.DistinctLocationsCount == 1) 

        #tempRDD = filteredData.rdd        
        #if filteredData.count() != 0:
        #if len(filteredData.head(1)) != 0:
         #   for row1 in filteredData.rdd.collect():
          #     for row2 in gymLocations.rdd.collect():
           #        print(row1);
            #       print(row2);
             #      if (row1.Latitude - row2.latitude) < EPSILON and (row1.Longitude - row2.longitude) < EPSILON:
              #         riskedUsers = tempRDD.filter(lambda x: x != row1)
               #        tempRDD = riskedUsers
 
        #Select all the heart rate values for the current window of the risked users
        displayRDD = stillUsersRDD.join(df,"deviceID")
        print("Count of filtered data: ",displayRDD.count())

        
        #Append the date to the s3 bucket folder in order to collect data for each in different folder
	#As this data would be pushed in the redshift at the end of the day, naming the tables based on
	#date would help in controlling the size of the table in the database
        now = datetime.datetime.now()
        date = now.strftime("%Y-%m-%d")
        time_of_the_day = now.strftime("%H-%M-%S")


        #Add time field to the agrregated data
        groupedRDD = groupedRDD.withColumn("Time", lit(now)) 
              
        #Save the aggregated data to S3
        if  len(groupedRDD.head(1)) != 0:
            groupedRDD.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/aggregatedData_"+ date + "/")
         
        #Add the time field to the data
        #displayRDD = displayRDD.withColumn("Time",lit(now))

        #Save all the heart rate values for the risked users to S3 
        if len(displayRDD.head(1)) != 0:
            displayRDD.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/riskedUserData_" + date + "/")

        
        #if tempRDD.count() != 0:
        #if len(tempRDD.take(1)) == 0:
         #   print("tempRDD is empty")
        #else:
         #   tempRDD.write \
          #     .format("com.databricks.spark.csv") \
           #    .mode("append") \
           #    .save("s3n://anshu-insight/usersInGym/")

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

    
def writeToRedshift(rdd):
    #for record in rdd:
    #if(rdd.count() != 0):
    if rdd.isEmpty():
        print("redshift:RDD is empty")
    else:
        rdd = rdd.map(lambda x: x.split(",")).map(lambda x: [str(y) for y in x])
        df = rdd.toDF()

        df.write\
            .format("com.databricks.spark.redshift")\
            .option("url", "jdbc-url/dev?user=<userName>&password=<password>") \
            .option("dbtable", "device_data") \
            .option("forward_spark_s3_credentials",True)\
            .option("tempdir", "s3n://anshu-insight/tmp") \
            .mode("error") \
            .save()



if __name__ == "__main__":

    #Get the required parameters from the passed arguments 
    broker, topic, gymLocationFile, userData = sys.argv[1:]

    #Schema for the gym locations and user records data
    gymFileSchema = StructType([StructField("latitude", DoubleType()),StructField("longitude", DoubleType())])
    userDataSchema = StructType([StructField("deviceID", IntegerType()),StructField("minHeartRate", DoubleType()),StructField("maxHeartRate", DoubleType())])

    #Get the gym location data
    gymLocations = sqlContext.read \
                                .format("com.databricks.spark.csv") \
                                .schema(gymFileSchema) \
                                .option("header", "true") \
                                .option("mode", "DROPMALFORMED") \
                                .load(gymLocationFile)

    #Get the user records
    usersData = sqlContext.read \
                                .format("com.databricks.spark.csv") \
                                .schema(userDataSchema) \
                                .option("header", "true") \
                                .option("mode", "DROPMALFORMED") \
                                .load(userData)
 
    #Persist this data in memory to avoid reading from hdfs again and again.
    collectedGymLocations = gymLocations.rdd
    #collectedGymLocations.persist()
    userRecords = usersData.rdd
    #userRecords.persist()

    #Consume the data from Kafka and process it
    readFromKafkaAndProcess(topic,broker)


