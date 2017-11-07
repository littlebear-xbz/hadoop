# os.environ['JAVA_HOME'] = '/usr/java/jdk18/'
# os.environ['JRE_HOME'] = '/usr/java/jdk18/jre'
# os.environ['HADOOP_CONF_DIR'] = '/run/cloudera-scm-agent/process/217-yarn-NODEMANAGER/'
# os.environ['PYTHONPATH'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/python/lib/py4j-0.10.4-src.zip'
# os.environ['PATH'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/bin:' \
#                      '/usr/java/jdk18/bin:/usr/java/jdk18/bin:' \
#                      '/usr/java/jdk18/jre/bin/:/usr/lib64/qt-3.3/bin:' \
#                      '/usr/local/sbin:/usr/local/bin:/usr/sbin:' \
#                      '/usr/bin:/root/.local/bin:' \
#                      '/root/bin:' \
#                      '/usr/local/apache-maven-3.5.0/bin:' \
#                      '/opt/cloudera/phoenix-4.9.0-cdh5.9.1/bin:' \
#                      '/data/soft/new/kafka-eagle/bin:/root/bin'
# os.environ['CLASSPATH'] = '.:/usr/java/jdk1.8.0_121/lib/dt.jar:/usr/java/jdk1.8.0_121/lib/tools.jar'
# os.environ['LD_LIBRARY_PATH'] = ':/usr/local/lib'
# sys.path.append("/usr/java/jdk18/bin")
# sys.path.append("/usr/java/jdk18/jre/bin")
# os.environ['SPARK_DIST_CLASSPATH'] = '/run/cloudera-scm-agent/process/217-yarn-NODEMANAGER/:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/*'
# os.environ['SPARK_CLASSPATH'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/mysql-connector-java-5.1.42-bin.jar:'
# sys.path.append("/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/python")
# sys.path.append("/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/python/lib/py4j-0.10.4-src.zip")

import os
from pyspark import SparkContext
from pyspark import SparkConf


def testA():
    print "a"

def set_spark_env():
    os.environ['SPARK_HOME'] = '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2'
    os.environ['SPARK_DIST_CLASSPATH'] = '/run/cloudera-scm-agent/process/217-yarn-NODEMANAGER/:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/hadoop/libexec/../../hadoop-mapreduce/.//*'

if __name__ == '__main__':
    set_spark_env()
    conf = SparkConf()
    conf = conf.setAppName("test")
    sc = SparkContext(conf=conf)
    x = sc.parallelize(range(1, 5))
    y = sc.parallelize(range(6, 10))
    print x.zip(y).collect()
