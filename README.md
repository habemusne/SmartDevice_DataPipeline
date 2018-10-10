# Introduction
This project aims at building data pipeline for collecting data from smart health devices
like fitbit, apple watch etc and running analysis on the collected data. The fields/attributes 
of the collected data will contain the device ID, location(latitude, longitude), heart rate, number 
of hours slept, hours spent in running for each user of the device. The data will be processed 
in real time to detect anomaly in the heart rates of the patients who need their health to be monitored 
continously.

# Motivation
Chronic diseases such as heart disease are the major causes of sickness and health care costs in the nation.
The biggest areas of spending and concern are for coordination of care and preventing hospital admissions for 
people with chronic conditions. The wearable health devices that can monitor vital signs when combined with 
patients health records( for example desired state of the patients health after any surgery or in daily routine ) 
or  machine learning, can make it possible for doctors to rapidly take required actions to their patients cases.
It has the potential to enable scalable chronic disease management with better care at lower costs.

# Tools and technologies used 
1. Apache Kafka
2. Apache Spark Streaming
3. S3
4. AWS Redshift
5. Dash 

# Details of the data
There are three tables containing the information about the device. 

-> User_Details-contains the user personal details including device id, area zipcode of 
the user, normal heart beat range of the user as this might differ from person to person 
depending on various conditions. (User_Details)

-> Streaming Data-collects the streaming data from the device with its timestamp. The fields 
in the incoming data are device id, latitude of the current location,longitude of the current 
location,heart rate  and timestamp at which this data is collected.

-> Gym locations-contains the gym facility locations around the given region. The data from this 
table would be used to compare the location of the device to first confirm if its a false alarm.


# Flow of the data pipeline
![image Info](file:////Users/anshu/Desktop/datapipeline.png "Data Pipline")

 
The data will be read from the devices with the help of Kafka on regular intervals. This data will be fed to the Spark Streaming. If the averaged out value from a window of 2 minutes value from some device is deviating too much from the standard range of heart rate for that person ( present in User_Details), appropriate action would be taken for those users.
The aggregated data and the raw data is saved to S3 as its coming and being processed.

The device id of the patients with anomaly will be displayed on the Dash dashboard.

Scheduled jobs-

A scheduler would be running to copy the data from S3 to redshift. At the end of the day, the data collected during the day for all the users will be copied to  the data ware house.

Daily reports will be generated from the collected data. A comparison in the heart beat pattern over a week would 
be available to the doctors for further analysis in the patients health.

# Possible extension
The incoming data can be used with machine learning algorithms to give more accurate results while keeping the 
false alarms low.

# Further scope of improvements
Spark streaming does not guarantee efficiently handling late incoming data.This is very important in case of health 
monitoring devices. The current data pipeline filters out the late incoming data based on current time and the 
timestamp of the data. This can be further improved with Spark Structured Streaming by using watermarking mechanism. In Structured Spark Streaming, the time limit to accept the late incoming data can be specified. If the data does not come in that window, the data will be discarded and will not impact the result.
The support for Structured spark streaming was not available for python untill recently. The repo contains the code for
getting the data and saving the result to local machine. ForEachWriter function which is available for saving the data to
the database did not work well in python. 





