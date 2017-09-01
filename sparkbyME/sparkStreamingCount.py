from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


def start():
    sc=SparkContext("local[7]","KafkaWordCount")
    ssc=StreamingContext(sc, 1)
    ssc.checkpoint('hdfs://jp-hadoop-02/data/checkpoint')

    brokers="jp-hadoop-03:9092,jp-hadoop-04:9092,jp-hadoop-05:9092,jp-hadoop-06:9092,jp-hadoop-07:9092,jp-hadoop-08:9092,jp-hadoop-09:9092"

    topic="test_flume_kafka"

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(u'hello', 1), (u'world', 1)])


    lines = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers})

    dadasets = lines.map(lambda x:x[1]).map(lambda line:line.split(" "))\
                        .map(lambda x:x[0]).map(lambda ipaddr:(ipaddr,1))\
                        .updateStateByKey(updateFunc , initialRDD=initialStateRDD)
    dadasets.saveAsTextFiles("hdfs://jp-hadoop-02/data/result.txt")

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    start()

