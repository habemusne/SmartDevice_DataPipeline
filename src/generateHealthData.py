#!/usr/bin/python

import random

def main():
    with open('userLocationData.csv', 'r') as in_file:
        next(in_file)
        with open('deviceGeneartedData.csv', 'w') as out_file:
            for line in in_file:
                userId = line.split(',')[0]
                zip_code = line.split(',')[1]
                latitude = line.split(',')[2]
                longitute = line.split(',')[3]
                city = line.split(',')[4]
                
                num = 70
                #print(word)
                if(int(userId) < 20):
                    num = random.randint(60,70)

                elif(int(userId) < 40):
                    num = random.randint(80, 90)

                elif(int(userId) < 60):
                    num = random.randint(100, 120)

                elif(int(userId) < 80):
                    num = random.randint(80, 90)

                elif(int(userId) <= 100):
                    num = random.randint(40, 50)

                out_file.write(userId + ',' + zip_code + ',' + latitude + ',' + longitute + ',' + city + ',' + str(num) + '\n')

#Call the main function
main()
