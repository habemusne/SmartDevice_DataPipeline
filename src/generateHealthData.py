#!/usr/bin/python

import random
import datetime


def main():
    now = datetime.datetime.now()
    with open('userLocationData.csv', 'r') as in_file:
        next(in_file)
        with open('deviceGeneartedData.csv', 'w') as out_file:
            for line in in_file:
                deviceId = line.split(',')[0]
                zip_code = line.split(',')[1]
                latitude = line.split(',')[2]
                longitute = line.split(',')[3]
                city = line.split(',')[4]
                
                num = 70
                #print(word)
                if(int(deviceId) < 20):
                    num = random.randint(60,70)

                elif(int(deviceId) < 40):
                    num = random.randint(80, 90)

                elif(int(deviceId) < 60):
                    num = random.randint(100, 120)

                elif(int(deviceId) < 80):
                    num = random.randint(80, 90)

                elif(int(deviceId) <= 100):
                    num = random.randint(40, 50)

                out_file.write(deviceId + ',' + zip_code + ',' + latitude + ',' + longitute + ',' + city + ',' \
                                 + str(now.year) + ',' + str(now.month) + ','  + str(num) + '\n')


#Call the main function
main()
