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
3. Redis
4. Flask

# Data fields in the provided data
There are two tables containing the information about the device. 
->One table is static which contains the device id, area zipcode of the user of the device (registered address with the device company) and normal range of the blood pressure of the user of the device as this might differ from person to person depending on age and other such factors. (User_Details)
-> The other table will collect the steaming data from the device with its timestamp. The fields in this table are device id, latitude of the current location,longitude of the current location, blood pressure and timestamp at which this data is collected. (User_Health_Details)

# Flow of the data pipeline
The data will be read from the devices with the help of Kafka on regular intervals. This data will be fed to the Spark Streaming. If the blood pressure value from some device is deviating too much from the standard range of blood pressure that 
is present in the table (User_Details), appropriate action would be taken for those users.
A scheduler would be running to schedule the batch processing. At the end of the day, the data collected during the day for all the locations (based on the saved area zipcodes in User_Details table) will be processed to identify the regions with maximum number of people with blood pressure problems. This report would be shown in the form of heat map.




