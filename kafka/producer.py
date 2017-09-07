# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
producer = KafkaProducer(bootstrap_servers=['jp-bigdata-05:9092', 'jp-bigdata-06:9092'])
count = 0
while count < 10000:
    # content = datetime.datetime.now()
    count = count + 1
    # line = str(content) + "::::::当前发送第" + str(count) + "条" + ":::::from pycharm"
    line = "test" + "   count:"+str(count)
    producer.send(topic='ltest_2',value=line)
    print line
    time.sleep(0.5)
producer.flush()
