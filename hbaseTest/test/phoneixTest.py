# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""
from kafka import KafkaConsumer
import phoenixdb
database_url = 'http://jp-bigdata-01:8765/'
# database_url = 'http://jh-hadoop-02:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()
cnx = conn
# cursor.execute("""select * from "user_log_info" limit 5""")

# sql = """UPSERT INTO LTEST(url_send,status,date,result_1,rowkeybyme)
# VALUES(?,?,?,?,?)"""
# # ('http://139.219.102.23:8003/JojoAndPage.jpg','success','2017-07-06 ',
# # 'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485',
# # '123http://139.219.102.23:8003/JojoAndPage.jpg')
# recivied_url_send = 'http://139.219.102.23:8003/JojoAndPage.jpg'
# recivied_status = 'success'
# recivied_data = '2017-07-06'
# recivied_result_1 = 'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485'
# recivied_rowkey = '1http://139.219.102.23:8003/JojoAndPage.jpg'
# cursor.execute(sql ,(recivied_url_send,recivied_status,recivied_data,recivied_result_1,recivied_rowkey))
# print cursor.fetchall()

sql = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status,result_1)
VALUES('2http://139.219.102.23:8003/JojoAndPage.jpg',
'2http://139.219.102.23:8003/JojoAndPage.jpg',
'2017-09-13 12:12:12',
'success',
'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485'
)
"""
recived_txt = "5http://139.219.102.23:8003/JojoAndPage.jpg,success,http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485,http://wh-dev:8009/JojoAndPage/JojoAndPage_2/JojoAndPage_2.jpg|C:\zhiqian\Faces\PDB\436\06-snap0583.jpg|06-snap0583|0.6539416"


tmp = recived_txt.split(",")
url_send = tmp[0]
recived_status = tmp[1]
recived_results = ['','','','','','','','','','']
recived_results[0] = "ceshi123"
recived_results[1] = "ceshi1234"
sql_l = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status,result_1,result_2,result_3
,result_4,result_5,result_6,result_7,result_8,result_9,result_10)
VALUES('3http://139.219.102.23:8003/JojoAndPage.jpg',
'http://139.219.102.23:8003/JojoAndPage.jpg',
'2017-09-13 12:12:12',
'success',
'%(result_1)s','%(result_2)s','%(result_3)s','%(result_4)s','%(result_5)s','%(result_6)s',
'%(result_7)s','%(result_8)s','%(result_9)s','%(result_10)s'
)
""" % {"result_1":recived_results[0],"result_2":recived_results[1],"result_3":recived_results[2],
       "result_4": recived_results[3],"result_5":recived_results[4],"result_6":recived_results[5],
       "result_7": recived_results[6],"result_8":recived_results[7],"result_9":recived_results[8],
       "result_10": recived_results[9]}

sql_l2 = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status,result_1,result_2,result_3
,result_4,result_5,result_6,result_7,result_8,result_9,result_10)
VALUES('3http://139.219.102.23:8003/JojoAndPage.jpg',
'http://139.219.102.23:8003/JojoAndPage.jpg',
'2017-09-13 12:12:12',
'success',
'%(result_1)s','%(result_2)s','%(result_3)s','%(result_4)s','%(result_5)s','%(result_6)s',
'%(result_7)s','%(result_8)s','%(result_9)s','%(result_10)s'
)
""" % {"result_1":recived_results[0],"result_2":recived_results[1],"result_3":recived_results[2],
       "result_4": recived_results[3],"result_5":recived_results[4],"result_6":recived_results[5],
       "result_7": recived_results[6],"result_8":recived_results[7],"result_9":recived_results[8],
       "result_10": recived_results[9]}


sql_l = 'select * from test.ltest'


# print recived_results
cursor.execute(sql_l)
for i in cursor.fetchall():
    print i


