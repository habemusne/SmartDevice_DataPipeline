import sys
import json
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row

#Initialize the contexts#
sc = SparkContext(appName="HealthInsights")
ssc = StreamingContext(sc, 120) # 2 min window
sqlContext = SQLContext(sc)
EPSILON = 0.01


def findAvgHeartRatePerDevice(rdd):
    #Dont do anything if the rdd is NULL
    if rdd.count() != 0:
        df = rdd.toDF()
        print("Schema of the dataframe:")
        df.printSchema()
	
	#root
 	#|-- _1: long (nullable = true)
 	#|-- _2: string (nullable = true)
	#|-- _3: string (nullable = true)
 	#|-- _4: double (nullable = true)
 	#|-- _5: long (nullable = true)
        
        #Cast the columns to double for airthmatic operations on them
        df = df.withColumn("_2",col("_2").cast("double"))
        df = df.withColumn("_3",col("_3").cast("double"))
        

        #Average out the heart rate for each device id in the current window.
	groupedRDD = df.groupBy("_1").agg(func.round(mean("_2"),4).alias("Longitude"),func.round(mean("_3"),4).alias("Latitude"),func.round(mean("_5"),0).alias("avgHeartRate"))
	print("Schema of the grouped dataframe:")
        groupedRDD.printSchema()
        print(groupedRDD.columns)
        groupedRDD = groupedRDD.selectExpr("_1 as deviceID", "Longitude as Longitude","Latitude as Latitude","avgHeartRate as avgHeartRate")

        
        #Join the existing records of user  with the incoming data
        userRecordsDF = userRecords.toDF()
        #joinedDF = groupedRDD.join(userRecordsDF, groupedRDD.deviceID == userRecordsDF.deviceID, how='inner')
        joinedDF = groupedRDD.join(userRecordsDF, "deviceID")
        #print("joinedData")
        #print(joinedDF.head(2))
        #print("joined data = ", joinedDF.count())

        #Filter out those users who have not their heart rates in the desired range by comparing it with their existing health records
        filteredData = joinedDF.filter((joinedDF.avgHeartRate < joinedDF.minHeartRate) | (joinedDF.avgHeartRate > joinedDF.maxHeartRate))

        #print("filteredData")
        #print(filteredData.head(2))
        #print("filtered data = ", filteredData.count())

        tempRDD = filteredData        
        if filteredData.count() != 0:
            for row1 in filteredData.rdd.collect():
               for row2 in gymLocations.rdd.collect():
                   print(row1);
                   print(row2);
                   if (row1.Latitude - row2.latitude) < EPSILON and (row1.Longitude - row2.longitude) < EPSILON:
                       riskedUsers = tempRDD.filter(lambda x: x != row1)
                       tempRDD = riskedUsers
 
        #Select all the heart rate values for the current window of the risked users
        displayRDD = filteredData.join(df,"deviceID")
        
        #Save the filtered data to S3
        if filteredData.count() != 0:
            filteredData.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/averagedOutData/")

        #Save all the heart rate values for the risked users to S3 
        if displayRDD.count() != 0:
            displayRDD.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/displayData/")

        
        if tempRDD.count() != 0:
            tempRDD.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/usersInGym/")

def saveToDatabase(rdd):
       if rdd.count() != 0:
             df = rdd.toDF()
	     #Save the raw data to S3. This data will be pushed to redShift dataware house once in a day.
             df.write \
               .format("com.databricks.spark.csv") \
               .mode("append") \
               .save("s3n://anshu-insight/rawData/")


def readFromKafkaAndProcess(topic,broker):
    inputFromKafka = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker})

    print("GOT HERE")
    
    #Extract the data from Kafka producer
    extractData = inputFromKafka.map(lambda x: json.loads(x[1]))

    extractData.pprint()
    
    #Map the data 
    mappedData = extractData.map(lambda x: (int(x["deviceId"]),(x["latitude"]),(x["longitude"]), x["time"],int(x['heartRate'])))
    
    #Save the data
    mappedData.foreachRDD(saveToDatabase)

    #Process each incoming RDD
    mappedData.foreachRDD(findAvgHeartRatePerDevice)

    ssc.start()
    ssc.awaitTermination()

    
def writeToRedshift(rdd):
    #for record in rdd:
    if(rdd.count() != 0):
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
    collectedGymLocations.persist()
    userRecords = usersData.rdd
    userRecords.persist()

    #Consume the data from Kafka and process it
    readFromKafkaAndProcess(topic,broker)


