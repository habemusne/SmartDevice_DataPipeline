from time import sleep
from json import dumps
from kafka import KafkaProducer
#from configuration import CLUSTER_NODES

#producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))

producer = KafkaProducer(bootstrap_servers=['ec2-18-211-244-128.compute-1.amazonaws.com:9092','ec2-34-204-54-91.compute-1.amazonaws.com:9092','ec2-18-215-43-58.compute-1.amazonaws.com:9092','ec2-54-88-66-190.compute-1.amazonaws.com:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))

with open('deviceGenerated_org.csv', 'r') as in_file:
    next(in_file)
    for line in in_file:
        producer.send('part-test', value=line)
        sleep(1)

