import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafka")
    ssc = StreamingContext(sc, 120) # 120 second window
    broker, topic = sys.argv[1:]
    #inputFromKafka = KafkaUtils.createStream(ssc,broker,"raw-event-streaming-consumer",{topic:1})

    inputFromKafka = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker})
    print("GOT HERE")

    print(inputFromKafka)

    lines = inputFromKafka.map(lambda x: x[1])
    inputFromKafka.saveAsTextFiles("output","csv")

    ssc.start()
    ssc.awaitTermination()
