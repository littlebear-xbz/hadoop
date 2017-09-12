# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import sys
from kafka import KafkaConsumer
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
server_list = ['jp-bigdata-03:9092','jp-bigdata-04:9092','jp-bigdata-05:9092','jp-bigdata-06:9092','jp-bigdata-07:9092','jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']

consumer = KafkaConsumer('msreply', group_id='groupltest',
                         bootstrap_servers=server_list)
def listenTopic():
    count = 0
    for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    #  e.g., for unicode: `message.value.decode('utf-8')`
        count = count + 1
        print("count:%d topic:%s partition:%d offset:%d: key=%s value=%s"%(count,message.topic, message.partition,message.offset,
                                           message.key,
                                           message.value))

listenTopic()
