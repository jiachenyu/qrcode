#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import pymysql
import pyodbc
import time
import pprint
from prettytable import PrettyTable

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def makeSqlServerConn():
    connStr  = 'DRIVER={SQL Server};SERVER=139.196.57.30;PORT=1443;UID=%s;PWD=%s;DATABASE=%s;' % ('sa', 'windjack123', 'wifi2com')
    print("start setup odbc connection, connStr = {}".format(connStr), end='\n\n')
    return pyodbc.connect(connStr)

def makeMySqlConn():
    return pymysql.connect(host='localhost',user='root',password='hJexOiChC40H',db='qrcode',cursorclass=pymysql.cursors.DictCursor)
    
def writeDataToMessageUpLog(sqlServerConn, index):
    odbcCursor = sqlServerConn.cursor()
    deviceId = int(time.strftime("%y%m%d%H%M%S") + str(index))
    port = 4001
    length = 629
    data = '0x20202020202020202020202020202020202020200D0A202020202020202020D0C7D3EEB3ACCAD0CAD5D2F8C8EDBCFEC3E2B7D1B0E60D0A20202020202020202020202020202020202020200D0A3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D0D0AC1F7CBAEBAC53A3230313730333136303030392020202020323031372F30332F31362031343A30320D0AC6B7C3FB20202020202020202020202020202020B5A5BCDB2020CAFDC1BF202020202020BDF0B6EE0D0A2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D0D0AB2E2CAD4200D0A3132333231333231202020202020202020202038382E3030202020203120202020202038382E30300D0A2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D0D0ABACFBCC63A20312020202020202020202020202020202020202020202020202020202038382E30300D0AD3C5BBDDBDF0B6EE3A202020202020302E303020D3A6CAD5BDF0B6EE3A20202020202038382E30300D0ACAD5BFEEBDF0B6EE3A202020202038382E303020D5D2C1E3BDF0B6EE3A20202020202020302E30300D0A3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D3D0D0A2020202020202020BBB6D3ADB9E2C1D9A3ACD0BBD0BBBBDDB9CB21202020200D0A2020202020202020CDF8D6B7A3BA7777772E78696E677975736F66742E636F6D0D0A2020202020202020A3D1A3D1A3BA3134313636383330373620202020202020200D0A20202020202020202020202020202020202020200D0A1B69'
    odbcCursor.execute("insert into MessageUpLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 1))", deviceId, port, length, data)
    odbcCursor.commit()
    print("finish writeDataToMessageUpLog, the deviceId = {}!!!".format(deviceId), end='\n\n')
    return deviceId

def checkMessageDownLog(sqlServerConn, deviceId):
    odbcCursor = sqlServerConn.cursor()
    rows = odbcCursor.execute("select * from MessageDownLog where DeviceId = {}".format(deviceId)).fetchall()
    print("running \"select * from MessageDownLog where DeviceId = {}\"".format(deviceId), end='\n\n')
    print("running checkMessageDownLog: rows = {}!!!".format(len(rows)), end='\n\n')
    columns = [column[0] for column in odbcCursor.description]
    table = PrettyTable()
    table.field_names = columns
    [table.add_row(row) for row in rows]
    table.del_column(5)
    print(table)

def checkMySql(mysqlConn, deviceId):
    mysqlCursor = mysqlConn.cursor()
    mysqlCursor.execute("select * from qrcode_table where equ_id = {}".format(deviceId))
    print("running checkMySql: rows = {}".format(mysqlCursor.rowcount), end='\n\n')
    print("running: \"select * from qrcode_table where equ_id = {}\"".format(deviceId))
    rows = mysqlCursor.fetchall()
    columns = ['data_id', 'equ_id', 'link', 'status']
    table = PrettyTable()
    table.field_names = columns
    result = []
    for row in rows:
        res = []
        for column in columns:
            res.append(row[column])
        result.append(res)
    [table.add_row(row) for row in result]
    print(table)
    
    
if __name__ == '__main__':
    numbers = int(raw_input(u"Please input the number of data which you want add: "))
    sqlServerConn = makeSqlServerConn()
    mysqlConn = makeMySqlConn()
    deviceIds = [writeDataToMessageUpLog(sqlServerConn, index) for index in xrange(numbers)]
    time.sleep(20)
    [checkMessageDownLog(sqlServerConn, deviceId) for deviceId in deviceIds]
    [checkMySql(mysqlConn, deviceId) for deviceId in deviceIds]
    raw_input()