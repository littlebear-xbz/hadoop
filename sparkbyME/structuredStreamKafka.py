# -*- coding: utf-8 -*-
# ----------------------------------------------------------------
# 功能：实时统计ip地址数量
# 实现：flume 获取 日志记录，并将数据推送至kafka中
#       使用 structured streaming 处理实时数据
# 编写人： chenyangang
# 日期： 2017-08-31
# ----------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

bootstrapServers="jp-bigdata-03:9092,jp-bigdata-04:9092,jp-bigdata-05:9092,jp-bigdata-06:9092,jp-bigdata-07:9092,jp-bigdata-08:9092,jp-bigdata-09:9092"

def structured_streaming_kafka(spark):
    lines = spark.read.format("kafka")\
                  .option("kafka.bootstrap.servers", bootstrapServers)\
                  .option("subscribe", ["test_flume_kafka"])\
                  .load()\
                  .selectExpr("CAST(value AS STRING)" )
    
    print(lines.value)
    ipaddr = lines.select(explode(split(lines.value, ' ')).alias('ipaddr'))

    ipaddrCounts = ipaddr.groupBy('ipaddr').count()

    query = ipaddrCounts\
             .write\
             .mode('append')\
             .format('console')\
             .start()

    query.awaitTermination()


if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()


    structured_streaming_kafka(spark)




