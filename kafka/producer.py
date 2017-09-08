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
producer = KafkaProducer(bootstrap_servers=['jp-bigdata-05:9092', 'jp-bigdata-09:9092'])

count = 0
while count < 1:
    line = "first+::::" + str(count)
    producer.send(topic='ltest_3',value=line)
    print line
    time.sleep(0.1)
    count = count+1
producer.flush()