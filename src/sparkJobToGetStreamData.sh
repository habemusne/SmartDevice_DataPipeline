#Include kafka, redshift packages and the topic name as spark-submit job parameters

/usr/local/spark/bin/spark-submit --jars RedshiftJDBC42-no-awssdk-1.2.16.1027.jar --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0 sparkStreaming.py localhost:9092 part-test
