#!/bin/sh

#The script contains commands to setup the cluster enviroment


KAFKA_CLUSTER=kafka-cluster
SPARK_CLUSTER=insight-cluster
DISPLAY_CLUSTER=display-cluster

source ~/.bash_profile

#kafka setup
peg up setup/spark/master_kafka.yml
peg up setup/spark/workers_kafka.yml
peg fetch ${KAFKA_CLUSTER}
peg install ${KAFKA_CLUSTER} ssh
peg install ${KAFKA_CLUSTER} aws
peg install ${KAFKA_CLUSTER} environment
peg sshcmd-cluster ${KAFKA_CLUSTER} "sudo apt-get install bc"
peg install ${KAFKA_CLUSTER} zookeeper
peg service ${KAFKA_CLUSTER} zookeeper start
peg install ${KAFKA_CLUSTER} kafka
peg service ${KAFKA_CLUSTER} kafka start
peg sshcmd-cluster ${KAFKA_CLUSTER} "sudo pip install kafka"


#hadoop and spark setup
peg up setup/spark/master.yml
peg up setup/spark/workers.yml
peg fetch ${SPARAK_CLUSTER}
peg install ${SPARK_CLUSTER} ssh
peg install ${SPARK_CLUSTER} aws
peg install ${SPARK_CLUSTER} environment
peg sshcmd-cluster ${SPARK_CLUSTER} "sudo apt-get install bc"
peg install ${SPARK_CLUSTER} hadoop
peg service ${SPARK_CLUSTER} hadoop start
peg install ${SPARK_CLUSTER} spark
peg service ${SPARK_CLUSTER} spark start


#dash setup
peg up setup/spark/masteri_display.yml
peg up setup/spark/workers_display.yml
peg fetch ${DISPLAY_CLUSTER}
peg install ${DISPLAY_CLUSTER} ssh
peg install ${DISPLAY_CLUSTER} aws
peg install ${DISPLAY_CLUSTER} environment
peg sshcmd-cluster ${DISPLAY_CLUSTER} "sudo pip install dash"
peg sshcmd-cluster ${DISPLAY_CLUSTER} "sudo pip install boto3"

#In /usr/local/spark/conf/spark-env.sh
#export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop

#S3 Bucket configurations
aws s3api create-bucket --bucket anshu-insight --region us-east-1



