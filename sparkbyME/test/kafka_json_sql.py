# coding: utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os
import sys
import json

reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.master("local[2]").appName("kafkaTest").config(conf=SparkConf()).getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 2)
zookeeper = "jp-bigdata-02:2181, jp-bigdata-03:2181, jp-bigdata-06:2181"
topic = "ltest"

initDstream = KafkaUtils.createStream(ssc=ssc, zkQuorum=zookeeper, groupId="groupltest",
                                      topics={topic: 1})

df = initDstream.map(lambda x: x[1].replace(u""""cltz":"",""", ""))


def processRDD(rdd):
    if rdd.isEmpty() is False:
        showschem = spark.read.json(rdd)
        showschem.select("take").show()


df.foreachRDD(processRDD)

ssc.start()
ssc.awaitTermination()
