from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg


## TODO: The directory  


#Initialize spark
spark = SparkSession.builder.appName("Identify-Potential-Locations").getOrCreate()

data = (spark.read
    .format("com.databricks.spark.csv")
    .options(inferSchema="true", delimiter=",", header="true")
    .load("deviceGeneartedData.csv"))

data.printSchema()

#Filter people with Blood Pressure greater than 90
filteredData = data.filter(data['BloodPressure'] > 120 or data['BloodPressure'] < 70)

#Group the high blood pressure by their location
groupedData = filteredData.groupBy("Latitude","Longitude").count()
groupedData.show()

groupedData.write.save("potentialLocations.csv")


