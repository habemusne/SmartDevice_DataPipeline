# Introduction

Health monitoring services grow along with the increasing popularity of wearable devices. The amount of health data sent to such services is usually enormous. Maintainability is an important factor to keep such services available and extensible.

[The original HeartWatch](https://github.com/anshu3769/SmartDevice_DataPipeline) is a project developed by a previous [Insight](https://www.insightdatascience.com/) fellow. It aims to handle the large-scale health data using Kafka + Spark streaming. Embedding these two major technologies, it requires engineers with in-depth knowledge in both to maintain. This is a human resource requirement that a company may not readily have. My project, the new HeartWatch, aims to resolve this problem by simplifying the architecture to Kafka-only.

# The Problem Defined

Same as the original project, this project mainly answers this question by using data engineering techniques:

> Given all users' historical range of their heart rates, and given the real-time stream of their current location + heart rate.
> Find out those whose heart rates are out of their historical ranges, but are not moving during the last X seconds.

# The Data

All data in this project are randomly generated using [avro random generator](https://github.com/confluentinc/avro-random-generator). There are mainly two data sets, historical and real-time. The historical data contains each user's historical heart rate range, and the real-time data contains each user's location and heart rate at the moment. Their schemas are [here](https://github.com/habemusne/heart_watch/blob/master/schemas/historical.avsc) and [here](https://github.com/habemusne/heart_watch/blob/master/schemas/realtime_value.avsc) respectively.

In the current set up, the historical data has 20,000 users (20,000 rows in turn).


# The Pipeline Shift

![alt text](https://github.com/habemusne/heart_watch/blob/master/presentation/architecture_shift.png "Pipeline Shift")

The main goal of this project is to remove the Spark streaming module and use Kafka only. Therefore, it does not take too much care for the data downstreams. While the original project includes S3 and Redshift as the downstream, this project simply uses Kafka Connect's S3 sink connector to store the processed data on S3. Another mild difference is that this project uses ReactJS rather than Dash as the frontend.

# A Closer Look

![alt text](https://github.com/habemusne/heart_watch/blob/master/presentation/internal.png "Internal")

This project splits the system into 3 clusters including a broker cluster, a KSQL cluster, a producer cluster. It also contains several other services including a database, a Kafka connect service, several schema registry services, a Confluent dashboard, and a frontend website. Except for the producer cluster and the website, all other clusters and services are dockerized and their  configurations are at [docker-compose.yml](https://github.com/habemusne/heart_watch/blob/master/docker-compose.yml).

### Broker cluster

The broker cluster serves as the backbone of this project. In my own setup, I use 3 `r5a.large` machines. Each machine runs a broker and a zookeeper.

### KSQL cluster

The KSQL cluster works on the analytics processing of the data. In my own setup, I use 3 `r5.large` machines. Each machine runs 1 KSQL server and 1 KSQL shell client.

### Producer cluster

The producer cluster is used to send real-time data to the broker cluster. In my own setup, I use 3 `t2.xlarge` machines. Each machine can have X producer processes where X is configurable as the `NUM_PRODUCERS_PROCESSES_PER_MACHINE` env. Each process reads the whole real-time data file and send each message (as avro) to the brokers. For demo purpose, once they are invoked, they will constantly run until the env `PRODUCER_RUN` is set to `0`.

However, the producer cluster is not necessary for running this project.

If env `MODE` is set to `dev`, then the project will use [DatagenConnector](https://github.com/confluentinc/kafka-connect-datagen) to stream random real-time data. Keep in mind that this connector cannot run in multi-process.

If env `MODE` is set to `prod`, but `PRODUCER_LIST` is not set, then the brokers will be used as producers as default.

### Database

The postgres database hosts historical data. The project uses the Kafka Connect service to bring them into the system. Currently, the database is hosted on one of the brokers.

### Kafka Connect

Kafka Connect brings historical data into the system. If env `MODE` is set to `dev`, then it is also in charge of generating random real-time data to the system. Currently, Kafka Connect is hosted on one of the brokers, and there can only be 1 instance.

### Schema Registry

Schema registry ensures that the system can handle consistent data formats. Currently it is hosted on one of the brokers and all of the producers. The reason why a producer needs schema registry is that it uses [kafka-avro-console-producer](https://docs.confluent.io/current/schema-registry/serializer-formatter.html) to generate the data. This producer requires schema registry to ensure data format. By default, they all use the schema-registry deployed locally, which is why we need multiple schema registry services. It is certainly [possible](https://stackoverflow.com/questions/55478598/why-kafka-avro-console-producer-doesnt-honour-the-default-value-for-the-field) that they all use the schema registry service hosted on one of the brokers. Due to time constraint, I did not implement this feature to reduce the amount of services.

### Confluent Dashboard

The Confluent Dashboard is used to monitor, operate, and manage the brokers. It also allows to run KSQL queries. Currently, it is hosted on one of the brokers.

### The Website

The website module mainly serves to display simply metrics of the KSQL tables/streams during the system running. It consists of a simple [Flask backend](https://github.com/habemusne/heart_watch/blob/master/ui/server.py) and a [ReactJS frontend](https://github.com/habemusne/heart_watch/tree/master/ui).

# Data Flow

As aforementioned, the historical data is conveyed by Kafka Connect to the system. It is constructed as a KSQL table from the topic it goes to.

The source of real-time data differs between `dev` and `prod` mode. In `dev`, the `DataGenConnector` generates random data on-the-fly. In `prod`, producers are in charge of reading a `jsonlines` real-time data file and send each line to the system. The details are shown in the two diagrams below. Note that the former cannot be multi-processed.

In either case, the real time data goes to a single topic, the real-time topic. The KSQL servers then brings the data from this topic to a stream. Another stream is created from this stream in order to repartition the data by the correct key.

Finally, KSQL joins the historical data with the real-time data, publishing to the final topic. The website displays the metrics of both the first stream (in order to show producer's message-producing speed) and the final table (in order to show KSQL's message-consumption speed).

### When `MODE=dev`

![alt text](https://github.com/habemusne/heart_watch/blob/master/presentation/data_flow_dev.png "Dev Data Flow")


### When `MODE=prod`

![alt text](https://github.com/habemusne/heart_watch/blob/master/presentation/data_flow.png "Prod Data Flow")

# Performance Improvement

According to the author, the original project handles ~14,000 messages with 3 m4.large brokers and 3 m4.large Spark workers ($0.6/hr). This project handles ~16,000 messages with 3 r5a.large brokers and 3 r5.large ksql servers ($0.717/hr) with 100 partitions and 30 producer processes. This is a 14% increase.

About the measurement: I measure it in a very simple manner. I counted the elapsed time from when the first stream shows non-0 messages-per-second to when the final table shows 0 messages-per-second. The elapsed time is about 3 minutes, which means 16,000 messages on average.

# Future Improvements

1. Enable Zookeeper cluster. Currently, all services use a single Zookeeper. The Zookeeper cluster mode is not set up. This can become serious in production when it becomes unavailable.
2. Explore performance improvements. The system can now run with ease, making performance experiments easy. It currently does not process too many data per second, which is a potential area for more experiments.
3. Improve code reusability. Currently this project serves only for health data analytics. It'd be great if it can handle more analytic tasks.

# Project Setup

Please visit [this page](https://github.com/habemusne/heart_watch/blob/master/SETUP.md) for project setup instructions.
