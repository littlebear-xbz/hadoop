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

Hbase_url = "jp-bigdata-03"
Hbase_port = 9090

transport = TTransport.TBufferedTransport(TSocket.TSocket(Hbase_url, Hbase_port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)


def creat_table():
    transport.open()
    table_name = "l_test_table"
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
transport.open()

print client.getRow('l_test_table','1307354998804454')
transport.close()