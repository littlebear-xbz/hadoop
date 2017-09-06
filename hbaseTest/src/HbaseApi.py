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

'''
创建表
'''
def creat_table(table_name = "l_test_table"):
    transport.open()
    content_1 = Hbase.ColumnDescriptor(name='person:', maxVersions=2)
    content_2 = Hbase.ColumnDescriptor(name='content:', maxVersions=2)
    client.createTable(table_name,[content_1,content_2])
    print client.getTableNames()
    transport.close()

'''
删除表
'''
def delete_table():
    transport.open()
    client.disableTable("l_test_table")
    client.deleteTable("l_test_table")
    transport.close()

'''
获取表名
'''
def get_table_names():
    transport.open()
    names = client.getTableNames()
    transport.close()
    return names


'''
清空表
'''
def truncate(table_name="l_test_table",row="person"):
    transport.open()
    client.deleteAllRow(table_name,row)
    transport.close()

"""
print getARow(colnum="person:name")
"""

def getARow(table="hdfs_hbase",row='1884915421804564',colnum='person:address'):
    transport.open()
    result = client.get(table,row,colnum)
    transport.close()
    return result[0].value


if __name__ == "__main__" :
    print getARow(colnum="person:name")
