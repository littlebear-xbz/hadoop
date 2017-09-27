# coding=utf-8
"""
直接存入hbase数据库

"""

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from kafka import KafkaConsumer
import hashlib
import datetime
# server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口
transport = TSocket.TSocket('jp-bigdata-02', 9090)
# 可以设置超时
transport.setTimeout(5000)
# 设置传输方式（TFramedTransport或TBufferedTransport）
trans = TTransport.TBufferedTransport(transport)
# 设置传输协议
protocol = TBinaryProtocol.TBinaryProtocol(trans)
# 确定客户端

# 打开连接


server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']

kafka_consumer = KafkaConsumer('msreply', group_id='groupltest',bootstrap_servers=server_list)

# 处理接收到的一行数据
str = "1http://139.219.102.23:8003/JojoAndPage.jpg,success,\
http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485,\
http://wh-dev:8009/JojoAndPage/JojoAndPage_2/JojoAndPage_2.jpg|C:\zhiqian\Faces\PDB\436\06-snap0583.jpg|06-snap0583|0.6539416"

HBASE_TABLE_NAME = 'TAB_MSFACEREC_RECIVIED'

def recived_process(str=''):
    list = ['' for i in range(13)]
    result_recived_list = str.split(',')
    for i,line in enumerate(result_recived_list):
        list[i] = result_recived_list[i]
    return list,result_recived_list

def safe_to_hbase():
    hbase_client = Hbase.Client(protocol)
    transport.open()
    result_list,result_recived_list = recived_process(str)
    rowkey = hashlib.md5(result_list[0]).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
             + result_list[0]
    recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    result_list_result = [0,1,
                 Hbase.Mutation(column="recived:result_1", value=result_list[2]),
                 Hbase.Mutation(column="recived:result_2", value=result_list[3]),
                 Hbase.Mutation(column="recived:result_3", value=result_list[4]),
                 Hbase.Mutation(column="recived:result_4", value=result_list[5]),
                 Hbase.Mutation(column="recived:result_5", value=result_list[6]),
                 Hbase.Mutation(column="recived:result_6", value=result_list[7]),
                 Hbase.Mutation(column="recived:result_7", value=result_list[8]),
                 Hbase.Mutation(column="recived:result_8", value=result_list[9]),
                 Hbase.Mutation(column="recived:result_9", value=result_list[10]),
                 Hbase.Mutation(column="recived:result_10", value=result_list[11]),
                          ]

    mutations = [Hbase.Mutation(column="recived:url_send", value=result_list[0]),
                 Hbase.Mutation(column="recived:status", value=result_list[1]),
                 Hbase.Mutation(column="recived:time_recived", value=recived_time),
                 ]
    if len(result_recived_list) > 2:
        print len(result_recived_list)
        i = 2
        while i <= len(result_recived_list) - 1:
            mutations.append(result_list_result[i])
            print mutations
            i = i + 1


    hbase_client.mutateRow(HBASE_TABLE_NAME, rowkey, mutations)
    transport.close()

safe_to_hbase()
