# -*- coding: utf-8 -*-
# ----------------------------------------------------------------
# 功能：实时统计ip地址数量
# 实现：flume 获取 日志记录，并将数据推送至kafka中
#       spark streaming 按RDD处理数据
#       处理后数据写入redis中进行统计
#       从redis中将数据写入到mysql
# 编写人： chenyangang
# 日期： 2017-08-02
# ----------------------------------------------------------------
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis
import datetime
import MySQLdb


class WriteToMySQL(object):
    cursor = None
    cnx = None

    def __init__(self):
        self.initConnect()

    def initConnect(self):
        try:
            cnx = MySQLdb.connect(host='jp-mysql-01', user='root', passwd='bigdata', db='jpdb', port=3306)
            print("-------------------Sucessful--------------------------")
        except MySQLdb.connection as err:
            print("Failed creating database: {}".format(err))
            exit(1)

        self.cursor = cnx.cursor()
        self.cnx = cnx
        return self.cursor, self.cnx

    def insertTable(self, statis_date, statis_hour, ipaddr, login_cnt):
        add_static_result = ("INSERT INTO ads_log_static_ipaddr "
                             "(statis_date, statis_hour, ipaddr, login_cnt) "
                             "VALUES (%s, %s,  %s, %s)")
        self.cursor.execute(add_static_result, (statis_date, statis_hour, ipaddr, login_cnt))
        print("insert data sucessfully !!-------------")
        self.cnx.commit()

    def updateTable(self, login_cnt, statis_date, statis_hour, ipaddr):
        update_static_result = ("UPDATE  ads_log_static_ipaddr "
                                "SET login_cnt = %s"
                                "WHERE statis_date = %s AND statis_hour = %s AND ipaddr=%s")
        self.cursor.execute(update_static_result, (login_cnt, statis_date, statis_hour, ipaddr))
        print("update data sucessfully !!-------------")
        self.cnx.commit()

    def insertDetail(self, ipaddr, user_id, login_time, url, write_time):
        add_detail_log = (" INSERT INTO dwd_log_login_d "
                          "(ipaddr, user_id, login_time, url, write_time)"
                          "VALUES (%s, %s,  %s, %s, %s)")
        self.cursor.execute(add_detail_log, (ipaddr, user_id, login_time, url, write_time))
        self.cnx.commit()


class RedisClient(object):
    pool = None
    writeToMySQL = None

    def __init__(self):
        self.getRedisPool()
        self.writeToMySQL = WriteToMySQL()

    def getRedisPool(self):
        redisip = 'jp-mysql-01'
        redisPort = 6379
        redisDB = 0
        self.pool = redis.ConnectionPool(host=redisip, port=redisPort, db=redisDB)
        return self.pool

    def addToHashSet(self, statis_date, statis_hour, key, value):
        if self.pool is None:
            self.pool = self.getRedisPool()
        r = redis.Redis(connection_pool=self.pool)
        hashSetName = "my80-log-iphash-" + statis_date

        flag = False
        if r.exists(hashSetName) is False:
            flag = True

        if r.hexists(hashSetName, str(key)):
            r.hincrby(hashSetName, str(key), value)

            print(r.hget(hashSetName, str(key)), statis_date, statis_hour, str(key))
            # 将结果更新至mysql
            self.writeToMySQL.updateTable(r.hget(hashSetName, str(key)), statis_date, statis_hour, str(key))
        else:
            r.hset(hashSetName, str(key), value)

            print(statis_date, statis_hour, str(key), r.hget(hashSetName, str(key)))
            # 将结果更新至mysql
            self.writeToMySQL.insertTable(statis_date, statis_hour, str(key), r.hget(hashSetName, str(key)))

        if flag is True:
            r.expire(hashSetName, 3600 * 24 + 300)

    def addToList(self, dealtime, value):
        if self.pool is None:
            self.pool = self.getRedisPool()
        r = redis.Redis(connection_pool=self.pool)
        key = 'my80-log-list'
        r.expire(key, 3600 * 24 + 300)
        r.lpush(key, value)

        print(value['ipaddr'], value['user_id'], value['login_time'], value['url'])
        # 将明细数据写入mysql
        self.writeToMySQL.insertDetail(value['ipaddr'], value['user_id'], value['login_time'], value['url'], dealtime)


def initSparkStreaming():
    sc = SparkContext("local[7]", "KafkaWordCount")
    ssc = StreamingContext(sc, 2)
    # ssc.checkpoint('hdfs://jp-hadoop-02/data/checkpoint')

    # set zookeeper
    zookeeper = "jp-bigdata-02:2181, jp-bigdata-03:2181, jp-bigdata-06:2181"

    # set kafka's topic
    topic = "test_flume_kafka"

    initDstream = KafkaUtils.createStream(ssc=ssc, zkQuorum=zookeeper, groupId="kafka-streaming-redis",
                                          topics={topic: 1})
    return ssc, initDstream


def processRDD(rdd):
    initRDD = rdd.map(lambda x: x[1]).map(lambda line: line.split(" "))
    staticRDD = initRDD.map(lambda x: x[0]).map(lambda ipaddr: (ipaddr, 1)) \
        .reduceByKey(lambda a, b: a + b)

    detailRDD = initRDD.map(lambda line: {'ipaddr': line[0],\
                                          'user_id': line[2],\
                                          'login_time': line[3][1:],\
                                          'url': line[10] \
                                          })
    return staticRDD, detailRDD


def main():
    # 连接spark streaming流
    ssc, initDstream = initSparkStreaming()

    # 处理RDD
    staticRDD, detailRDD = processRDD(initDstream)

    # 数据写入redis中并进行计算
    r = RedisClient()

    # 统计ip地址次数,并将统计结果写入mysql
    def ipHandle(time ,rdd):
        statis_date = datetime.datetime.now().strftime("%Y-%m-%d")
        statis_hour = datetime.datetime.now().strftime("%H")

        if rdd.isEmpty() is False:
            # rddstr = "{"+','.join(rdd.collect())+"}"
            for element in rdd.collect():
                r.addToHashSet(statis_date, statis_hour, element[0], element[1])

    staticRDD.foreachRDD(ipHandle)

    # 明细数据写入redis和mysql
    def handleItem(time, rdd):
        dealtime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if rdd.isEmpty() is False:
            for element in rdd.collect():
                r.addToList(dealtime, element)

    detailRDD.foreachRDD(handleItem)

    # 处理结束，关闭streaming
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()

