# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import sys
from kafka import KafkaConsumer

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('ltest',
                            bootstrap_servers=['jp-hadoop-05:9092', 'jp-hadoop-06:9092',
                                                'jp-hadoop-07:9092', 'jp-hadoop-08:9092', 'jp-hadoop-09:9092'])
for message in consumer:
# message value and key are raw bytes -- decode if necessary!
#  e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s"%(message.topic, message.partition,message.offset,
                                       message.key,
                                       message.value))
