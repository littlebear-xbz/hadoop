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
import os
from hdfs import Client

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation
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
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host=THRIFTSERVER, port=THRIFTPORT))

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
        found = False

        for table in tables:
            if table == self.tablename:
                found = True

        # 删除表
        if found is True:
            self.deleteTable()
            # 创建表
            self.client.createTable(self.tablename, [ColumnDescriptor(name=self.columnFamily, maxVersions=1)])

    def deleteTable(self):
        self.client.disableTable(self.tablename)
        self.client.deleteTable(self.tablename)


    # 导入数据
    def importData(self, deal_file_handle):
        # Create a list of mutations per work of Shakespeare
        mutations_batch = []
        # batch site
        batch_size = 100

        for line in deal_file_handle:
            record = line.split(" ")

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
            # 一次提交多行
            mutations_batch.append(BatchMutation(row=rowkey, mutations=mutations))
            if len(mutations_batch) % batch_size == 0:
                self.client.mutateRows(self.tablename, mutations_batch)
                mutations_batch = []


if __name__ == "__main__":

    # 建立hbase连接
    hbasewriteer = CreateTableAndImportData('user_log_info', 'cf_log')
    hbasewriteer.createTable()

    # 连接HDFS
    client = Client(HDFSNN)

    # 获取文件列表
    logFiles = client.list(LOGPATH)

    # 读取文件
    for logfile in logFiles:
        with client.read(os.path.join(LOGPATH, logfile) ) as deal_file_handle:
            hbasewriteer.importData(deal_file_handle)
