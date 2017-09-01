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
producer = KafkaProducer(bootstrap_servers=['jp-hadoop-05:9092', 'jp-hadoop-06:9092',
                                            'jp-hadoop-07:9092', 'jp-hadoop-08:9092', 'jp-hadoop-09:9092'])

while 1:
    producer.send(topic='ltest',value='first')
    print 'seng first'
    time.sleep(1)