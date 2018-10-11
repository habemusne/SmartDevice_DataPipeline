
General information about the elements of the pipeline:

The pipeline consists of three clusters.
1. Spark cluster
2. Kafka cluster
3. AWS Redshift cluster
Both the clusters are built in AWS cloud. The Spark cluster has 4 nodes. The kafka cluster has 3 nodes.
There is one EC2 instance for running front end for the pipeline. 

Steps to build the data pipeline:-
1. Setup the Spark and Kafka Clusters using the commands provided in setup/environment.sh file.
2. Create an S3 bucket on AWS for storing the incoming data
3. Create the redshift cluster follwing the AWS documentation from the link - https://docs.aws.amazon.com/ses/latest/DeveloperGuide/event-publishing-redshift-cluster.html
4. Clone the github directory on both the Spark and Kafka clusters.
5. On the Kafka cluster, create topin on the Kafka cluster by running the SmartDevice_DataPipeline/src/scripts/create-kafka-topic.sh script from command line. 
	->Go the directory SmartDevice_DataPipeline/src/scripts/
	->Run the command ./create-kafka-topic.sh
6. On the Kafka cluster, start the data generation process by running the SmartDevice_DataPipeline/src/kafka/generateHealthData.py script 
	-> Go the directory SmartDevice_DataPipeline/src/kafka/
	-> Run the command python generateHealthData.py <broker:port> <partition no>
	-> Run the above command on all the nodes of the cluster with each instance sending to different partitions

7. On the spark cluster, run the spark job for processing the data
	->Go to the directory SmartDevice_DataPipeline/src/scripts/
	-> Run the command ./sparkJobToGetStreamData.sh
It will start collecting the data from the Kafka producer and run processing on the collected data.

8. On the front end machine (one of the EC2 instances), 
	->Go to the directory SmartDevice_DataPipeline/src/dash/
	-> Run the command to run the front end- sudo python displayPlotly.py

9. On the same machine (being used for front end), make the entry in the crontab from the SmartDevice_DataPipeline/src/cronJob/entryInCrontab.txt. 
It will start a cron job to copy the data from s3 bucket to Redshift once in a day.
