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
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

count = 0
while count < 100000:
    # line = "first+::::from windows::" + str(count)
    line = 'http://wh-dev:8009/Test/001.jpg'
    producer.send(topic='mssend', value=line)
    print line
    time.sleep(0.5)
    count = count+1
producer.flush()
