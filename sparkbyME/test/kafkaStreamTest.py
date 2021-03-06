# coding: utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
import json

reload(sys)
sys.setdefaultencoding('utf8')


sc = SparkContext("local[2]", "kafkaTest")
ssc = StreamingContext(sc, 2)
zookeeper = "jp-bigdata-02:2181, jp-bigdata-03:2181, jp-bigdata-06:2181"
topic = "ltest"

initDstream = KafkaUtils.createStream(ssc=ssc, zkQuorum=zookeeper, groupId="groupltest",
                                      topics={topic: 1})

df = initDstream.map(lambda x: x[1].replace(u""""cltz":"",""", ""))


def processjson(i):
    jsonmessage = json.loads(i)
    return jsonmessage['take']


df.map(processjson).pprint()
ssc.start()
ssc.awaitTermination()
