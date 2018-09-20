import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

def writeToRedshift(rdd):
    #for record in rdd:
        if(rdd.count() != 0):
            rdd = rdd.map(lambda x: x.split(",")).map(lambda x: [str(y) for y in x])
            df = rdd.toDF()

            df.write\
            .format("com.databricks.spark.redshift")\
            .option("url", "jdbc:redshift://<redshift-cluster-url>:5439/dev?user=<userName>&password=<pswrd>") \
            .option("dbtable", "device_data") \
            .option('forward_spark_s3_credentials',True)\
            .option("tempdir", "s3n://bucket") \
            .mode("error") \
            .save()



if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafka")
    ssc = StreamingContext(sc, 60) # 60 second window

    sqlSpark = SparkSession \
    .builder \
    .appName("PythonStreamingRecieverKafka") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    # hadoop configurations for storing data to redshift
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",<ACCESS-KEY-ID>)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",<SECRET-ACCESS-KEY>)

    broker, topic = sys.argv[1:]
    #inputFromKafka = KafkaUtils.createStream(ssc,broker,"raw-event-streaming-consumer",{topic:1})

    inputFromKafka = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker})
    print("GOT HERE")

    print(inputFromKafka)

    lines = inputFromKafka.map(lambda x: x[1])


  ## Real time processing of the incoming data.
    ## Get the current location of the users having blood pressure values in abnormal range.
    ## TO DO : - Check the values against the values you have in your user details and then
    ## create report based on that comparison.
    ## Schema of the data is -  DeviceId,Zipcode,Latitude,Longitude,City,Year,Month,Blood Pressure

   # with open('AbnormalDataReport.csv', 'w') as out_file:
    #    out_file.write("Latitude," + "Longitude,"+ "BloodPressure")
        #split the data into fields
        #for line in lines:
    #    lines.foreachRDD(lambda rdd: rdd.split(","))
     #       field, count = lines.split(',')
      #      if(field[8] > 120):
       #         out_file.write(field[2] + ',' + field[3] + ',' + field[8] + '\n')


    ## Save the data for batch processing
    lines.saveAsTextFiles("output","csv")


    ## Write to the Redshift DB
    #lines.foreachRDD(lambda rdd: rdd.foreachPartition(writeToRedshift))
    lines.foreachRDD(writeToRedshift)

    ssc.start()
    ssc.awaitTermination()


