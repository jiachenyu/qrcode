#! /usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import logging
import hashlib
import pymysql
import time
import os
import sys
import shortuuid
import urllib
import urlparse
import pyodbc
import pprint
import binascii
import psutil
from logging.handlers import RotatingFileHandler

reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s:%(lineno)-5s - %(levelname)s - %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

rotateFile = RotatingFileHandler('main.log',maxBytes=10 * 1024 * 1024,backupCount=5)
rotateFile.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)-5s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
rotateFile.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(rotateFile)

def performance(f):
    def fn(*args, **kw):
        start = time.time()
        result = f(*args, **kw)
        end = time.time() 
        logger.info('call %s() in %fs' % (f.__name__, (end - start)))
        return result
    return fn

def ensureOneProcessInstance():
    pids = psutil.pids()
    processes = [psutil.Process(pid) for pid in pids]
    processInstances = [process for process in processes if (process.name().find('main')) == 0 and (process.name().find('.exe') > 0)]
    return processInstances
        
#-------------------- sql server operation ----------------------
dsn  = 'sqlserverdatasource'
user = 'sa'
password = 'windjack123'
database = 'wifi2com'
connStr  = 'DRIVER={SQL Server};SERVER=139.196.57.30;PORT=1443;UID=%s;PWD=%s;DATABASE=%s;' % (user, password, database)
logger.info("start setup odbc connection, connStr = {}".format(connStr))
odbcConn   = pyodbc.connect(connStr)
odbcCursor = odbcConn.cursor()
logger.info("odbc connection setup successfully!")


def getNewRowsFromUpLog():
    rows = odbcCursor.execute("select * from MessageUpLog where IsNew = 1 order by CreatedOn desc").fetchall()
    logger.info("running getNewRowsFromUpLog: rows = {}".format(len(rows)))
    return rows


def updateIsNewFromUpLog(row):
    odbcCursor.execute("update MessageUpLog set IsNew = 0 where Id = ?", row[0])
    odbcCursor.commit()
    logger.info("running updateIsNewFromUpLog: update MessageUpLog set IsNew = 0 where Id = {}".format(row[0]))


def doWriteDownLog(upLogRow, frontText, behindText):
    deviceId = upLogRow[1]
    length   = upLogRow[3]
    data = upLogRow[4]
    port = choose(upLogRow[2] == 4000, 4001, 4000)
    #odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)",deviceId,port,length,data)
    #odbcCursor.commit()

    frontText = frontText.encode('gb2312').encode('hex').upper()
    length = len(frontText) / 2
    odbcCursor.execute(
        "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))",
        deviceId,
        port,
        length,
        frontText)
    odbcCursor.commit()

    behindText = behindText.encode('gb2312').encode('hex').upper()
    length = len(behindText) / 2
    #odbcCursor.execute(
    #    "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))",
    #    deviceId,
    #    port,
    #    length,
    #    behindText)
    #odbcCursor.commit()
    logger.info("finish doWriteDownLog!")


def doWriteDownLogForApiError(upLogRow, errorText):
    deviceId  = upLogRow[1]
    length    = upLogRow[3]
    data      = upLogRow[4]
    port      = choose(upLogRow[2] == 4000, 4001, 4000)
    logger.info("finish doWriteDownLogForApiError! errorText = {}".format(errorText))
    errorText = errorText.encode('gb2312').encode('hex').upper() + "0A"
    length    = len(errorText) / 2
    odbcCursor.execute(
        "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))",
        deviceId, port, length, errorText)
    odbcCursor.commit()
    
#-------------------- mysql operation ----------------------
logger.info("start setup mysql connection.")
mysqlConn = pymysql.connect(host='localhost',
                            user='root',
                            password='hJexOiChC40H',
                            db='qrcode',
                            cursorclass=pymysql.cursors.DictCursor)
logger.info("mysql connection setup successfully!")

def findStatus2(dataId):
    mysqlConn.ping(True)
    mysqlConn.autocommit(True)
    sqlStr = "select * from qrcode_table where status = 2"
    with mysqlConn.cursor() as cursor:
        for index in xrange(30):
            cursor.execute(sqlStr)
            logger.info("running handleMysqlStatus: {}, dataId = {}, index = {}, rows number is {}".format(sqlStr, dataId, index, cursor.rowcount))
            if cursor.rowcount > 0:
                return list(cursor)[:1]
            else:
                time.sleep(0.2)
    return []

lastSuccessRow = None


def handleMysqlStatus(upLogRow):
    dataId   = upLogRow[0]
    deviceId = upLogRow[1]
    port     = choose(upLogRow[2] == 4000, 4001, 4000)

    rows = findStatus2(dataId)
    
    global lastSuccessRow
    if len(rows) > 0:
        row      = rows[-1]
        blobData = row['data_blob']
        length   = len(blobData)
        blobData = pyodbc.Binary(blobData)
        #odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))", deviceId, port, length, blobData)
        odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId, port, length, blobData)
        odbcCursor.commit()
        
        blobData1 = row['data_blob1']
        if blobData1:
            length1 = len(blobData1)
            blobData1 = pyodbc.Binary(blobData1)
            odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId, port, length1, blobData1)
            odbcCursor.commit()
        
        blobData2 = row['data_blob2']
        if blobData2:
            length2 = len(blobData2)
            blobData2 = pyodbc.Binary(blobData2)
            odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId, port, length2, blobData2)
            odbcCursor.commit()
        
        sqlStr = "update qrcode_table set status = 3 where data_id = {}".format(dataId)
        mysqlConn.cursor().execute(sqlStr)
        logger.info("running handleMysqlStatus: {}".format(sqlStr))
        mysqlConn.commit()
        lastSuccessRow = row
    else:
        sqlStr = "SELECT * FROM qrcode_table WHERE status=3 ORDER BY data_id DESC LIMIT 0,1"
        cursor = mysqlConn.cursor()
        if not lastSuccessRow:
            cursor.execute(sqlStr)
            result = cursor.fetchall()
            if len(result) == 0:
                return
            logger.info("running handleMysqlStatus: {}".format(sqlStr))
            lastSuccessRow = result[0]

        blobData = lastSuccessRow['data_blob']
        length   = len(blobData)
        blobData = pyodbc.Binary(blobData)
        logger.info("blobData length: mysql = {}, mssql = {}".format(length, len(blobData)))
        odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId,port,length,blobData)
        odbcCursor.commit()
        doWriteDownLogForApiError(upLogRow, "QR Error")
        
        blobData1 = lastSuccessRow['data_blob1']
        if blobData1:
            length1   = len(blobData1)
            blobData1 = pyodbc.Binary(blobData1)
            odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId, port, length1, blobData1)
            odbcCursor.commit()
        
        blobData2 = lastSuccessRow['data_blob2']
        if blobData2:
            length2   = len(blobData2)
            blobData2 = pyodbc.Binary(blobData2)
            odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)", deviceId, port, length2, blobData2)
            odbcCursor.commit()
            
        sqlStr = "update qrcode_table set status = 4 where data_id = {}".format(dataId)
        mysqlConn.cursor().execute(sqlStr)
        logger.info("running handleMysqlStatus: {}".format(sqlStr))
        mysqlConn.commit()
    logger.info("finish handleMysqlStatus!")

#-------------------- request and store picture operation ----------------
qrCodeDir = os.path.abspath(os.path.join(os.path.dirname(__file__), "picture"))
if not os.path.exists(qrCodeDir):
    os.mkdir(qrCodeDir)
logger.info(os.path.abspath(qrCodeDir))

SIGNKEY = 'i5OqMrNXVyOJ5GEMYoEtRHqN1P9ghk6I'
URL = 'http://qiye.wxsdc.ediankai.com/api/v1/suppliers/1/staff/1/box/get'

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('http://', adapter)


def saveToDisk(url, data_id, equ_id):
    ticket = dict(
        urlparse.parse_qsl(
            urlparse.urlsplit(url).query)).get('ticket')
    filename = '{}_{}.jpeg'.format(data_id, equ_id)
    urllib.urlretrieve(url, os.path.join(qrCodeDir, filename))
    return open(os.path.join(qrCodeDir, filename), 'rb').read()


def doGetRequest(row):
    SIGNKEY = 'i5OqMrNXVyOJ5GEMYoEtRHqN1P9ghk6I'
    data_id, equ_id = row[0], row[1]

    parameterStr = "{SIGNKEY}data_id{data_id}equ_id{equ_id}{SIGNKEY}".format(**locals())
    logger.info("parameter str: {}".format(parameterStr))
    sign = hashlib.md5(parameterStr).hexdigest()
    data = {'equ_id': equ_id, 'data_id': data_id, 'sign': sign}
    logger.info("payload: {}".format(data))

    try:
        r = session.post(URL, data=data)
        if r.status_code == requests.codes.ok:
            url = r.json()['data']['url']
            frontText  = r.json()['data']['front_text']
            behindText = r.json()['data']['behind_text']
            logger.info(url)
            logger.info(frontText)
            logger.info(behindText)

            imageData = saveToDisk(url, data_id, equ_id)
            mysqlConn.ping(True)
            mysqlConn.cursor().execute(
                "INSERT INTO qrcode_table (data_id, equ_id, link, image) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE link=%s, image=%s",
                (data_id,
                 equ_id,
                 url,
                 imageData,
                 url,
                 imageData))
            mysqlConn.commit()
            return (url, frontText, behindText)
        else:
            logger.error("request failed! status_code={}".format(r.status_code))
            return None
    except requests.ConnectionError as exception:
        logger.error(exception, exc_info=True)
        return None
    except Exception as exception:
        logger.error(exception, exc_info=True)
        doWriteDownLogForApiError(row, "API Error")
        updateIsNewFromUpLog(row)
        logger.info("response text: {}".format(r.text))
        return None


def choose(res, left, right):
    logger.info("running choose: {}, {}, {}".format(res, left, right))
    return left if res else right
    
lastUpLogRow = None

import dateutil.parser
def shouldHandle(lastUpLogRow, upLogRow):
    #if upLogRow[2] == 4000:
    #    logger.info("skip port = 4000!")
    #    return False
        
    if not lastUpLogRow:
        logger.info("lastUpLogRow is None!")
        return True
    elif lastUpLogRow[1] != upLogRow[1]:
        logger.info("[DeviceId] is different!")
        return True
    elif (lastUpLogRow[1] == upLogRow[1]) and ((upLogRow[5] -lastUpLogRow[5]).total_seconds() >= 10):
        logger.info("[CreatedOn] diff is {}!".format( (upLogRow[5] -lastUpLogRow[5]).total_seconds() ))
        return True
    else:
        return False
        
def job():
    global lastUpLogRow
    logger.info("start job!!!")
    rows = getNewRowsFromUpLog()
    for row in rows:
        if shouldHandle(lastUpLogRow, row):
            result = doGetRequest(row)
            if result:
                lastUpLogRow = row
                updateIsNewFromUpLog(row)
                doWriteDownLog(row, result[1], result[2])
                time.sleep(1)
                handleMysqlStatus(row)
        else:
            updateIsNewFromUpLog(row)
    logger.info("finish job!!!")

if __name__ == '__main__':
    processes = ensureOneProcessInstance()
    logger.info(len(processes))
    if len(processes) > 2:
        logger.info("This program has been started! The instance number is {}".format(len(processes)))
        for process in processes:
            logger.info("Thre process name is {}!".format(process.name()))
        logger.info("Please kill these processes mannually!")
        raw_input()
    else:
        while True:
            try:
                job()
            except Exception as exception:
                logger.error(exception, exc_info=True)
            time.sleep(1)
