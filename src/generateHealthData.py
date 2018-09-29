#!/usr/bin/python

import random
import sys
import math
import datetime
import json
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer

START = datetime.utcfromtimestamp(0)

class DataProducer(object):

    def __init__(self,address):
        self.dataProducer = KafkaProducer(bootstrap_servers=address)
    
    def timeInSecs(self,currTime):
        return (currTime - START).total_seconds()

    def produceData(self,sourceId):
         device_count = 1
         while True:
             with open('userLocationData.csv', 'r') as in_file:
                 next(in_file)
                 for line in in_file:
                    deviceId = str(device_count)
                    latitude = line.split(',')[2]
                    longitude = line.split(',')[3]
                    timeStamp = self.timeInSecs(datetime.now())
                
                    if(int(deviceId) < 200):
                        heartRate = random.uniform(60,70)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 400):
                        heartRate = random.uniform(80, 90)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 600):
                        heartRate = random.uniform(140, 200)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 800):
                        heartRate = random.uniform(80, 120)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) <= 1000):
                        heartRate = random.uniform(90, 180)
			heartRate = round(heartRate,2)
                    
                    data = json.dumps({"deviceId":deviceId,
                                     "time":timeStamp,
                                     "latitude":latitude,
                                     "longitude":longitude,
                                     "heartRate":heartRate})
                     
                    #print(data)
                    #print('\n')
                    self.dataProducer.send('device-data',data)
                    device_count+=1
                    if device_count%1000 == 0:
                        device_count = 1;
		    #sleep(0.1);

if __name__ == "__main__":
    args = sys.argv
    address = str(args[1])
    partition_id = str(args[2])
    producer = DataProducer(address)
    producer.produceData(partition_id)

