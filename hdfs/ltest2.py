# -*- coding: utf-8 -*-
"""

@author: xiongz
"""
from hdfs import Client
from hdfs import InsecureClient
import threading
import time

client = InsecureClient('http://jp-bigdata-03:50070', user='hdfs')

def writeToHdfs():
    while True:
        with client.write(hdfs_path="/user/xiongz/test.log", encoding='utf-8', append=True) as writer:
            writer.write("I am A\n")
        print "write I am A"
        time.sleep(2)

writeToHdfs()