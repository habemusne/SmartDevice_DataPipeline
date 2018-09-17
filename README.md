# Introduction
This project aims at building data pipeline for collecting data from smart health devices
like fitbit, apple watch etc and running analysis on the collected data. The fields/attributes 
of the collected data will contain location(latitude, longitude), blood pressure, number of hours
slept, hours spent in running for each user of the device. The data will be processed 
in two ways:
1. Real time processing - This processing will focus on monitoring the blood pressure of the user.
If the range of the blood pressure varies highly from the standard range (90-120), then user's emergency
contact will be connected.
2. Batch processing -  Queries will be run on the batch of the data to know the regions having
large number of people with blood pressure problems. The pharmaceuticals companies dealing
with blood pressure drugs can make use of this report to target potential market for the sale of their 
drugs. The other information that can be retrieved from this data is the regions where people spend
resonable time in running. The succes of events like marathon can be predited with this preliminary 
information along with other factors combined.

# Tools and technologies to be used 
1. Apache Kafka
2. Apache Spark
3. AWS S3
4. Redis
5. Flask


