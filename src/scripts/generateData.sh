# Comamnd to start data generation script
#This command will be run on kafka cluster instances to run data generation on multiple instances
# broker list = ec2-52-202-106-225.compute-1.amazonaws.com:9092, ec2-18-235-25-194.compute-1.amazonaws.com:9092,
# ec2-18-235-25-194.compute-1.amazonaws.com:9092
#partition ids = 0,1,2
python generateHealthData.py --broker ec2-52-202-106-225.compute-1.amazonaws.com:9092 --partition 1
