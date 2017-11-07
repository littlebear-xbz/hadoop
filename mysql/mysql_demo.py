# -*- coding: utf-8 -*-
import pymysql
import os
import time
conn = pymysql.connect(host='jp-mysql-01',port=3306,
                       db='jpdb',user='hive',passwd='hive')

cur = conn.cursor()
sql = "select human_id,credentials_num from jp_human_blacklist"
cur.execute(sql)
human_list = cur.fetchall()
count = 0
for human_list_id,human_list_card in human_list:
	path = './images/' + human_list_id + ".jpg"
	if os.path.isfile(path):
		count = count + 1
		if count % 100 == 0:
			print count
	else:
		print path + "is error"

print count
