# -*- coding: utf-8 -*-
"""
mysql test
"""

import jpype
import jaydebeapi

phoenix_client_jar="/opt/cloudera/phoenix-4.9.0-cdh5.9.1/phoenix-4.9.0-cdh5.9.1-client.jar"
args='-Djava.class.path=%s' % phoenix_client_jar
jvm_path=jpype.getDefaultJVMPath()
jpype.startJVM(jvm_path,args)
conn=jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                        'jdbc:phoenix:jp-bigdata-03:2181',
                        [],
                        phoenix_client_jar)
curs=conn.cursor()
sql="select * from test.ltest"
count=curs.execute(sql)
results=curs.fetchall()
for r in results:
  print r