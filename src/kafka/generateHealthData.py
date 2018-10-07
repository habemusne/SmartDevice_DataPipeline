#!/usr/bin/python

import random
import sys
import math
import datetime
import json
import time
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer
import argparse

START = datetime.utcfromtimestamp(0)

# Arguments settings
parser = argparse.ArgumentParser(description='Data generation')
parser.add_argument('--broker', type=str, default='ec2-18-235-25-194.compute-1.amazonaws.com:9092',
                    help="list of brokers")
parser.add_argument('--partition', type=int, default=0,
                    help="partition on which this producer should send")
args = parser.parse_args()

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
                    timeStamp = time.time()
                
                    if(int(deviceId) < 2000):
                        heartRate = random.uniform(60,65)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 4000):
                        heartRate = random.uniform(80, 85)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 6000):
                        heartRate = random.uniform(160, 165)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) < 8000):
                        heartRate = random.uniform(170, 175)
			heartRate = round(heartRate,2)

                    elif(int(deviceId) <= 10000):
                        heartRate = random.uniform(95, 100)
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
    address = str(args.broker)
    partition_id = str(args.partition)
    producer = DataProducer(address)
    producer.produceData(partition_id)

