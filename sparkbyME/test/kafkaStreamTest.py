#coding: utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os

os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2'
os.environ['SPARK_DIST_CLASSPATH'] = '/run/cloudera-scm-agent/process/217-yarn-NODEMANAGER/:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/.//*'

sc = SparkContext("local","kafkaTest")
ssc = StreamingContext(sc,2)
zookeeper = "jp-bigdata-02:2181, jp-bigdata-03:2181, jp-bigdata-06:2181"
topic = "ltest"

initDstream = KafkaUtils.createStream(ssc=ssc, zkQuorum=zookeeper, groupId="groupltest",
                                          topics={topic: 1})

print initDstream

ssc.start()
ssc.awaitTermination()