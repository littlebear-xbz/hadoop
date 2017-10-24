# -*- coding: utf-8 -*-
"""
test git
@author: xiongz
"""

from hdfs import InsecureClient
import pymysql
import sys
reload(sys)
sys.setdefaultencoding('utf8')
hdfs_client = InsecureClient("http://azure-mysql-07:50070","xiongz")
conn = pymysql.connect(host='azuer-mysql-06',port= 3306,user = 'hive',passwd='hive',db='jpdb',charset='utf8')
cur = conn.cursor()
with hdfs_client.read("/user/xiongz/data1.txt") as file:
    count = 0
    for line in file.read().split("\n"):
        list = line.split("|")
        sql = '''INSERT INTO l_test_person (name,sex,phoneNo,birthday,address,answer,keyNo) VALUES \
("%(name)s","%(sex)s","%(phoneNo)s","%(birthday)s","%(address)s","%(answer)s","%(keyNo)s")'''\
% {"name":list[0],"sex":list[1],"phoneNo":list[2],"birthday":list[3],"address":list[4],"answer":list[5],"keyNo":list[6]}
        cur.execute(sql.encode("utf8"))
        count = count + 1
        if count % 1000 == 0:
            print count
            conn.commit()
conn.commit()
cur.close()
conn.close()


