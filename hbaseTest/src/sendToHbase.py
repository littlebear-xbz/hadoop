# coding=utf-8
"""
for sent to Hbase
author: Littlebear

"""

"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
import datetime
from hbase.ttypes import *
from hdfs import InsecureClient
import sys
reload(sys)
sys.setdefaultencoding('utf8')

hdfs_client = InsecureClient("http://jp-bigdata-03:50070","xiongz")

Hbase_url = "jp-bigdata-03"
Hbase_port = 9090

transport = TTransport.TBufferedTransport(TSocket.TSocket(Hbase_url, Hbase_port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)


def creat_table(table_name = "l_test_table"):
    transport.open()
    content_1 = Hbase.ColumnDescriptor(name='person:', maxVersions=2)
    content_2 = Hbase.ColumnDescriptor(name='content:', maxVersions=2)
    client.createTable(table_name,[content_1,content_2])
    print client.getTableNames()
    transport.close()

def delete_table():
    transport.open()
    client.disableTable("l_test_table")
    client.deleteTable("l_test_table")
    transport.close()

def get_table_names():
    transport.open()
    names = client.getTableNames()
    transport.close()
    return names
    transport.close()

"""
从本地获取数据上次到Hbase，奇怪每次只能上传7w条数据
1条1条传数据
"""
def put_datas():
    transport.open()
    count = 0
    with open('data.txt') as file:
        for line in file:
            list = line.split('|')
            name = list[0]
            sex = list[1]
            phoneNo = list[2]
            birthDay = list[3]
            address = list[4]
            answer = list[5]
            rowkey = list[6].strip()
            mutations = [Hbase.Mutation(column="person:name",value=name),
                         Hbase.Mutation(column="person:sex", value=sex),
                         Hbase.Mutation(column="person:phoneNo", value=phoneNo),
                         Hbase.Mutation(column="person:birthDay", value=birthDay),
                         Hbase.Mutation(column="person:address", value=address),
                         Hbase.Mutation(column="content:answer", value=answer),
                        ]
            client.mutateRow('l_test_table',rowkey,mutations)
            count = count + 1
            if count % 100 == 0 :
                print count
            else:
                continue
    transport.close()

'''
超级慢的运行
1条1条传数据
'''
def put_datas_from_hdfs():
    transport.open()
    count = 0
    with hdfs_client.read("/user/xiongz/data.txt") as file:
        lines = file.read().split("\n")
        for line in lines:
            list = line.split('|')
            name = list[0]
            sex = list[1]
            phoneNo = list[2]
            birthDay = list[3]
            address = list[4]
            answer = list[5]
            rowkey = list[6].strip()
            mutations = [Hbase.Mutation(column="person:name", value=name),
                         Hbase.Mutation(column="person:sex", value=sex),
                         Hbase.Mutation(column="person:phoneNo", value=phoneNo),
                         Hbase.Mutation(column="person:birthDay", value=birthDay),
                         Hbase.Mutation(column="person:address", value=address),
                         Hbase.Mutation(column="content:answer", value=answer),
                         ]
            client.mutateRow('l_test_table', rowkey, mutations)
            count = count + 1
            if count % 100 == 0:
                print count
            else:
                continue
    transport.close()

'''
一次性全部提交
'''
def put_batch_from_local(table_name='l_test_table',batch_size=500):
    transport.open()
    mutations_batch = []
    with open('./data.txt') as file:
        lines = file.readlines()
        len_file = len(lines)
        print 'len_file : ' + str(len_file)
        i= 0
        for line in lines:
            i = i + 1
            list = line.split('|')
            name = list[0]
            sex = list[1]
            phoneNo = list[2]
            birthDay = list[3]
            address = list[4]
            answer = list[5]
            rowkey = list[6].strip()
            mutations = [Hbase.Mutation(column="person:name", value=name),
                         Hbase.Mutation(column="person:sex", value=sex),
                         Hbase.Mutation(column="person:phoneNo", value=phoneNo),
                         Hbase.Mutation(column="person:birthDay", value=birthDay),
                         Hbase.Mutation(column="person:address", value=address),
                         Hbase.Mutation(column="content:answer", value=answer),
                         ]
            mutations_batch.append(BatchMutation(row=rowkey, mutations=mutations))
            if batch_size >= len_file - i :
                print "start last mutateRows ----- i: " + str(i)
                client.mutateRows(table_name, mutations_batch)
                mutations_batch = []
            elif len(mutations_batch) % batch_size == 0 and batch_size <= len_file - i :
                print "start process mutateRows ----- i: " + str(i)
                client.mutateRows(table_name, mutations_batch)
                mutations_batch = []
            else:
                continue
    transport.close()

if __name__ == "__main__" :
    start = datetime.datetime.now()
    put_batch_from_local()
    end = datetime.datetime.now()
    print "time: " + str((end-start).seconds)