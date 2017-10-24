# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz

这是连接mysql的程序示例
"""
import pymysql
import time
import ConfigParser

def connect_mysql(CF):
    host = CF.get('mysql', 'host')
    port = CF.get('mysql', 'port')
    user = CF.get('mysql', 'user')
    password = CF.get('mysql', 'passwd')
    database = CF.get('mysql', 'db')
    conn = pymysql.connect(host=host, port=int(port), user=user, passwd=password, db=database)
    return conn

def select_test(conn):
    cur = conn.cursor()
    cur.execute("select * from ODS_MSFACEREC_RECIVED")
    all = cur.fetchall()
    for i in all:
        print i
        time.sleep(0.5)
    cur.close()

def main():
    CF = ConfigParser.ConfigParser()
    CF.read('../conf.conf')
    conn = connect_mysql(CF)
    select_test(conn)

if __name__ == '__main__':
    main()

