# -*- coding: utf-8 -*-
import pymysql

conn = pymysql.connect(host='jp-mysql-01',port=3306,
                       db='jpdb',user='hive',passwd='hive')

cur = conn.cursor()
sql = "select count(send_url) from ODS_MSFACEREC_RECIVED"
cur.execute(sql)
print cur.fetchone()[0]