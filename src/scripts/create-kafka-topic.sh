#Create the topic with replication factor, number of partitions and topic name as arguments to the command
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic device-data
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic anomaly-data
