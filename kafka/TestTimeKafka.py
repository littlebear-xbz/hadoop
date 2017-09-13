# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time
import logging
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='TestTimeKafka.log',
                    filemode='w'
                    )

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

count = 0
while count < 10000:
    # line = "first+::::from windows::" + str(count)
    line1 = 'http://wh-dev:8009/Test/001.jpg' + \
        ":::" + str(datetime.datetime.now())
    line2 = 'http://139.219.102.23:8003/JojoAndPage.jpg' + \
        ":::" + str(datetime.datetime.now())
    producer.send(topic='msreply', value=line1)
    producer.send(topic='msreply', value=line2)
    logging.info(line1)
    logging.info(line2)
    time.sleep(600)
    count = count+1
producer.flush()
