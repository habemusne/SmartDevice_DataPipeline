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

# Data fields in the provided data
There are three tables containing the information about the device. 
-> One table is static which contains the device id, area zipcode of the user of the device 
(registered address with the device company) and normal range of the blood pressure of the user of 
the device as this might differ from person to person depending on age and other such factors. (User_Details)
-> The other table will collect the steaming data from the device with its timestamp. The fields 
in this table are device id, latitude of the current location,longitude of the current location, 
heart rate  and timestamp at which this data is collected. (User_Health_Details)
-> The third table contains the gym facility locations around the given region. The data from this table would
be used to comapre the location of the device to first confirm if its a false alarm.


# Flow of the data pipeline
The data will be read from the devices with the help of Kafka on regular intervals. This data will be fed to the Spark Streaming. If the averaged out value from a window of 2 minutes value from some device is deviating too much from the standard range of heart rate for that person ( present in User_Details), appropriate action would be taken for those users.
A scheduler would be running to schedule the batch processing. At the end of the day, the data collected during the day for all the locations (based on the saved area zipcodes in User_Details table) will be processed to identify the regions with maximum number of people with heart problems. This report would be shown in the form of heat map.

# Possible extension
The incoming data can be used with machine learning algorithms to give more accurate results while keeping the 
false alarms low.

# Further scope of improvements
Spark streaming does not guarantee efficiently handling late incoming data. It can lead to erronous results.
This is very important in case of health monitoring devices. Spark Structured Streaming resolves this issue by
using watermarking mechanism. In Structured Spark Streaming, the time limit to accept the late incoming data can be 
specified. If the data does not come in that window, the data will be discarded and will not impact the result.





