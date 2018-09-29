#Include kafka, redshift packages, topic name  and input file paths as spark-submit job parameters


/usr/local/spark/bin/spark-submit --master spark://ec2-18-235-5-9.compute-1.amazonaws.com:7077  --jars RedshiftJDBC42-no-awssdk-1.2.16.1027.jar  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0 sparkDirectStream.py ec2-18-235-25-194.compute-1.amazonaws.com:9092 device-data /data/gymLocations.csv  /data/userRecords.csv
