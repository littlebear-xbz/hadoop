# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz

kafka-console-consumer --bootstrap-server  jp-bigdata-07:9092 --topic ltest --from-beginning
kafka-console-producer --broker-list jp-bigdata-07:9092 --topic ltest
"""
import sys
from kafka import KafkaConsumer
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='reply.log',
                    filemode='w'
                    )
# To consume latest messages and auto-commit offsets
server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092',
               'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
server_list_jh = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
                  'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092',]

consumer = KafkaConsumer('ltest', group_id='groupltest',bootstrap_servers=server_list)

def listenTopic():
    count = 0
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        #  e.g., for unicode: `message.value.decode('utf-8')`
        count = count + 1
        logging.info("count:%d topic:%s partition:%d offset:%d: key=%s value=%s" % (count, message.topic, message.partition, message.offset,
                                                                                    message.key,
                                                                                    message.value))
        print "count:%d topic:%s partition:%d offset:%d: key=%s value=%s" % (count, message.topic, message.partition, message.offset,
                                                                                    message.key,
                                                                                    message.value)
listenTopic()
