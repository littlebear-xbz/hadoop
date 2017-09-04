# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 10:11:08 2017

@author: chenyangang
测试环境中，将日志数据写入Hbase，为测试性能和开发准备数据
功能描述：
1、对用户访问信息加入用于rowkey
2、使用thrift写入Hbase
"""
import hashlib
import types
from hdfs import Client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import ColumnDescriptor, Mutation
from hbase import Hbase

# sys.path.append('C:\\Program Files\\Python 3.5\\Lib\\site-packages\\hbase\\ttypes.py')

# 日志文件所在路径
LOGPATH = '/data/sparkdev/data/weblogs/'

# HDFS namenode 地址
HDFSNN = 'http://jp-hadoop-02:50070'

# Thrift server addr
THRIFTSERVER = 'jp-hadoop-03'

# Thrift server port
THRIFTPORT = 9090


# -------------------------------------------------
# 功能描述：
#       根据输入字符串进行加密
#       若输入非字符串，默认加密"others"
# -------------------------------------------------
def md5(inputString, default="others"):
    if type(inputString) is types.StringType:
        encryptString = inputString
    else:
        encryptString = default
    m = hashlib.md5()
    m.update(encryptString)
    return m.hexdigest()


class CreateTableAndImportData(object):
    def __init__(self, tablename, columnFamily):
        # 表名
        self.tablename = tablename

        # 列族
        self.columnFamily = columnFamily

        # 建立和hbase的连接
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host=THRIFTSERVER, port=THRIFTPOR))

        # 通讯协议
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # 建立客户端
        self.client = Hbase.Client(self.protocol)

        # 建立通讯
        self.transport.open()

    # 关闭连接
    def __del__(self):
        self.transport.close()

    # 创建Hbase表
    def createTable(self):
        tables = self.client.getTableNames()

        columnFamily = ColumnDescriptor(name=self.columnFamily, maxVersions=1)

        self.client.createTable(self.tablename, [columnFamily])

        # 导入数据

    def importData(self, record):
        ipaddr = record[0]
        visitTime = record[3][1:]
        user_id = record[9]
        link = record[10]
        rowkey = md5(str(user_id) + str(visitTime))

        print(rowkey)

        mutations = [Mutation(column=self.columnFamily + ":ipaddr", value=ipaddr), \
                     Mutation(column=self.columnFamily + ":visitTime", value=visitTime), \
                     Mutation(column=self.columnFamily + ":user_id", value=user_id), \
                     Mutation(column=self.columnFamily + ":link", value=link)
                     ]
        self.client.mutateRow(self.tablename, rowkey, mutations)


if __name__ == "__main__":

    # 建立hbase连接
    hbasewriteer = CreateTableAndImportData('user_log_info', 'cf_log')
    hbasewriteer.createTable()

    # 连接HDFS
    client = Client(HDFSNN, timeout = 200000)

    # 获取文件列表
    logFiles = client.list(LOGPATH)

    # 读取文件
    for logfile in logFiles:
        with client.read(LOGPATH + logfile) as fp:
            for line in fp:
                record = line.split(" ")
                hbasewriteer.importData(record)
