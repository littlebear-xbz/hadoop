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
    data ="""{
"vechileID":"201711028060179",          
"kakouID":"160108110257057100",          
"deviceID":"160108110257057101",
"passTime":"2017-11-02 11:03:55.916",
"roadNum":"1",                          
"plateNum":"冀A61D71",
"plateColour":"2",                       
"backPlateNum":"-",
"backPlateColour":"4",
"plateMatch":"3",
"speed":"79",
"speedLimit":"40",
"vechileLength":"0",
"driveStatus":"0",
"vechileBrand":"大众",
"vechileLook":"",
"vechileColour":"J",
"vechileLogo":"",
"vechileKind":"1",
"plateType":"2",
"plateRegion":"",
"pictureNum":"1",
"jssj":"Thu Nov 02 2017 11:05:39 GMT+0800 (CST)",
"picturepath":["/home/app/apache-tomcat-6-datastore/webapps/Images5/2017/11/2/11/160108110257057101/160108110257057101_2017_11_02_11_03_55_916_1.jpg"],
"xxbh":"2017110280601790000000",
"Belt":"",
"Call":"",
"CDLX":"",
"WFZT":"",
"score":"90",
"cltz":"",
"rect":"92,1566,477,541",
"tagnum":"0",
"boxnum":"0",
"caryear":"20002007",
"dropnum":"0",
"id":"1797fbabebe356955372a86da13550ec7239491a",
"rksj":"",
"fxbh":" ",
"sbmc":"045阳光大道武汉纺织大学_1",
"type":"帕萨特",
"clzl":"1",
"sunflag":"0",
"clpp":"GZ0",
"carGNum":"冀",
"carANum":"A",
"carBNum":"6",
"carCNum":"1",
"carDNum":"D",
"carENum":"7",
"carFNum":"1",
"take":"EQ_045阳光大道武汉纺织大学（电警）",
"tx1":"http://100.11.44.110:8088/Images5/2017/11/2/11/160108110257057101/160108110257057101_2017_11_02_11_03_55_916_1.jpg"
}
"""
    producer.send(topic='ltest', value=data)
    print data
    time.sleep(0.5)
    count = count+1
    producer.flush()

