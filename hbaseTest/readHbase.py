# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017

@author: Administrator
"""

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase


transport = TTransport.TBufferedTransport(TSocket.TSocket('jp-hadoop-03', 9090))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)

#open
transport.open()
#get tables
tables = client.getTableNames()
print(tables)
#
#Get Column Descriptors
columns = client.getColumnDescriptors('user_log_info')
print columns
# for line in columns:
#     print line
#
# #
# #Get Table Regions
regions = client.getTableRegions('user_log_info')
print regions
#
#Row methods:Get Row
rows = client.getRow('user_log_info', '0fb371a5ee3bad9f487f1193634c5b10')
#
#
print rows

transport.close()