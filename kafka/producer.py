# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092', 'jp-bigdata-07:9092',
               'jp-bigdata-08:9092', 'jp-bigdata-09:9092']

server_list_jh = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
                  'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092',]

producer = KafkaProducer(bootstrap_servers=server_list)

count = 0
while count < 100000:
    # line = "first+::::from windows::" + str(count)
    data = 'http://100.37.1.172:8007/test.jpg'
    producer.send(topic='ltest', value=data)
    print data
    time.sleep(0.5)
    count = count+1
    producer.flush()

