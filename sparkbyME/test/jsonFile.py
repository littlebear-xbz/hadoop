# coding: utf-8
import os
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import time

os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2'
os.environ['SPARK_DIST_CLASSPATH'] = '/run/cloudera-scm-agent/process/217-yarn-NODEMANAGER/:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/.//*'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.json("/user/xiongz/json_test.txt")
df.select("take", "vechileID").show(2)
df.printSchema()