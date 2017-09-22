# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""
from kafka import KafkaConsumer
import phoenixdb
import logging
import hashlib
import datetime
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../log/safeToPhoenix.log',
                    filemode='w'
                    )

database_url = 'http://jp-bigdata-01:8765/'
# database_url = 'http://jh-hadoop-02:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()
cnx = conn

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']

# server_list = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
#                   'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092',]
consumer = KafkaConsumer('msreply', group_id='groupltest',bootstrap_servers=server_list)


for message in consumer:
    recived_message = message.value
    messagelist = recived_message.split(",")
    logging.info(messagelist)
    print messagelist
    if len(messagelist) == 2:
        if messagelist[1] == 'fail':
            logging.warning(messagelist[0]+"---url not Rec")
            recived_url_send = messagelist[0]
            recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            recived_status = messagelist[1]
            print str(messagelist) + "fail"
            rowkey =  rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S')\
                                            +recived_url_send
            sql_l = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status)
        VALUES('%(rowkey)s',
        '%(recived_url_send)s',
        '%(date)s',
        '%(status)s'
        )
        """ % {"rowkey":rowkey,'recived_url_send':recived_url_send,"date":recived_time,"status":recived_status}
            logging.info(sql_l)
            cursor.execute(sql_l)
        elif messagelist[1] == 'noAvator':
            logging.warning(messagelist[0]+'---not found face')
        elif messagelist[1] == 'noCardId':
            logging.warning(messagelist[0]+'---not VIP')
        pass
    elif len(messagelist) < 2 or len(messagelist) > 12:
        logging.error("message is error")
        logging.error(recived_message)
    elif len(messagelist) > 2 and len(messagelist):
        recived_url_send = messagelist[0]
        recived_status = messagelist[1]
        recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        recived_results = ['', '', '', '', '', '', '', '', '', '']
        rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S')\
        +recived_url_send
        logging.info(rowkey)
        logging.info("list lenth:::"+ str(len(messagelist)))
        for i in range(2,len(messagelist)):
            recived_results[i-2] = messagelist[i]
        sql_l = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status,result_1,result_2,result_3
        ,result_4,result_5,result_6,result_7,result_8,result_9,result_10)
        VALUES('%(rowkey)s',
        '%(recived_url_send)s',
        '%(date)s',
        '%(status)s',
        '%(result_1)s','%(result_2)s','%(result_3)s','%(result_4)s','%(result_5)s','%(result_6)s',
        '%(result_7)s','%(result_8)s','%(result_9)s','%(result_10)s'
        )
        """ % {"rowkey":rowkey,'recived_url_send':recived_url_send,"date":recived_time,"status":recived_status,
               "result_1": recived_results[0], "result_2": recived_results[1], "result_3": recived_results[2],
               "result_4": recived_results[3], "result_5": recived_results[4], "result_6": recived_results[5],
               "result_7": recived_results[6], "result_8": recived_results[7], "result_9": recived_results[8],
               "result_10": recived_results[9]}
        logging.info(sql_l)
        cursor.execute(sql_l)



