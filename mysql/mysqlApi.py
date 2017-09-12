# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import pymysql
import time

conn = pymysql.connect(host='192.168.1.55',port= 3306,user = 'root',passwd='bigdata',db='jpdb')
cur = conn.cursor()
reCount = cur.execute('select * from dwd_log_login_d')
all = cur.fetchall()
for i in all:
    print i
    time.sleep(0.5)
cur.close()
conn.close()