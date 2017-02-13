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
from logging.handlers import RotatingFileHandler

reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

rotateFile = RotatingFileHandler(
    'main.log',
    maxBytes=10 * 1024 * 1024,
    backupCount=5)
rotateFile.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotateFile.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(rotateFile)

#-------------------- sql server operation ----------------------
dsn = 'sqlserverdatasource'
user = 'sa'
password = 'windjack123'
database = 'wifi2com'
connStr = 'DRIVER={SQL Server};SERVER=139.196.57.30;PORT=1443;UID=%s;PWD=%s;DATABASE=%s;' % (
    user, password, database)
logger.info("start setup odbc connection, connStr = {}".format(connStr))
odbcConn = pyodbc.connect(connStr)
odbcCursor = odbcConn.cursor()
logger.info("odbc connection setup successfully!")


def getNewRowsFromUpLog():
    rows = odbcCursor.execute(
        "select * from MessageUpLog where IsNew = 1").fetchall()
    logger.info("running getNewRowsFromUpLog: rows = {}".format(len(rows)))
    return rows


def updateIsNewFromUpLog(row):
    odbcCursor.execute(
        "update MessageUpLog set IsNew = 0 where Id = ?",
        row[0])
    odbcCursor.commit()
    logger.info(
        "running updateIsNewFromUpLog: update MessageUpLog set IsNew = 0 where Id = {}".format(
            row[0]))


def doWriteDownLog(upLogRow, frontText, behindText):
    deviceId = upLogRow[1]
    length = upLogRow[3]
    data = upLogRow[4]
    port = choose(upLogRow[2] == 4000, 4001, 4000)
    odbcCursor.execute(
        "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)",
        deviceId,
        port,
        length,
        data)
    odbcCursor.commit()

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
    odbcCursor.execute(
        "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))",
        deviceId,
        port,
        length,
        behindText)
    odbcCursor.commit()
    logger.info("finish doWriteDownLog!")


def doWriteDownLogForApiError(upLogRow, errorText):
    deviceId = upLogRow[1]
    length = upLogRow[3]
    data = upLogRow[4]
    port = choose(upLogRow[2] == 4000, 4001, 4000)
    errorText = errorText.encode('gb2312').encode('hex').upper()
    length = len(errorText) / 2
    odbcCursor.execute(
        "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))",
        deviceId,
        port,
        length,
        errorText)
    odbcCursor.commit()
    logger.info("finish doWriteDownLogForApiError!")
#-------------------- mysql operation ----------------------
logger.info("start setup mysql connection.")
mysqlConn = pymysql.connect(host='localhost',
                            user='root',
                            password='hJexOiChC40H',
                            db='qrcode',
                            cursorclass=pymysql.cursors.DictCursor)
logger.info("mysql connection setup successfully!")

lastSuccessRow = None


def handleMysqlStatus(upLogRow):
    dataId = upLogRow[0]
    deviceId = upLogRow[1]
    port = choose(upLogRow[2] == 4000, 4001, 4000)

    cursor = mysqlConn.cursor()
    sqlStr = "select * from qrcode_table where status = 2 and data_id = {}".format(
        dataId)
    cursor.execute(sqlStr)
    rows = cursor.fetchall()
    logger.info(
        "running handleMysqlStatus: {}, rows number is {}".format(
            sqlStr, len(rows)))
    global lastSuccessRow
    if len(rows) > 0:
        row = rows[-1]
        blobData = row['data_blob']
        length = len(blobData)
        blobData = pyodbc.Binary(blobData)
        logger.info(
            "blobData length: mysql = {}, mssql = {}".format(
                length, len(blobData)))
        #odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))", deviceId, port, length, blobData)
        odbcCursor.execute(
            "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)",
            deviceId,
            port,
            length,
            blobData)
        odbcCursor.commit()
        mysqlConn.cursor().execute(
            "update qrcode_table set status = 3 where data_id = {}".format(dataId))
        logger.info("running handleMysqlStatus: {}".format(
            "update qrcode_table set status = 3 where data_id = {}".format(dataId)))
        mysqlConn.commit()
        lastSuccessRow = row
    else:
        if not lastSuccessRow:
            cursor.execute(
                "SELECT * FROM qrcode_table WHERE status=3 ORDER BY data_id DESC LIMIT 0,1")
            logger.info(
                "running: SELECT * FROM qrcode_table WHERE status=3 ORDER BY data_id DESC LIMIT 0,1")
            lastSuccessRow = cursor.fetchall()[0]

        blobData = lastSuccessRow['data_blob']
        length = len(blobData)
        blobData = pyodbc.Binary(blobData)
        logger.info(
            "blobData length: mysql = {}, mssql = {}".format(
                length, len(blobData)))
        #odbcCursor.execute("insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, convert(VARBINARY(max), ?, 2))", deviceId, port, length, blobData)
        odbcCursor.execute(
            "insert into MessageDownLog(DeviceId, Port, Length, Data) values(?, ?, ?, ?)",
            deviceId,
            port,
            length,
            blobData)
        odbcCursor.commit()
        doWriteDownLogForApiError(upLogRow, "QR Error")
        mysqlConn.cursor().execute(
            "update qrcode_table set status = 4 where data_id = {}".format(dataId))
        logger.info("running handleMysqlStatus: {}".format(
            "update qrcode_table set status = 4 where data_id = {}".format(dataId)))
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
    #data_id = shortuuid.ShortUUID(alphabet="0123456789").random(length=10)
    #equ_id  = shortuuid.ShortUUID(alphabet="0123456789").random(length=10)
    data_id, equ_id = row[0], row[1]

    parameterStr = "{SIGNKEY}data_id{data_id}equ_id{equ_id}{SIGNKEY}".format(
        **locals())
    logger.info("parameter str: {}".format(parameterStr))
    sign = hashlib.md5(parameterStr).hexdigest()
    data = {'equ_id': equ_id, 'data_id': data_id, 'sign': sign}
    logger.info("payload: {}".format(data))

    try:
        r = session.post(URL, data=data)
        if r.status_code == requests.codes.ok:
            url = r.json()['data']['url']
            [logger.info(element) for element in (url, r.json()['data'][
                'front_text'], r.json()['data']['behind_text'])]
            imageData = saveToDisk(url, data_id, equ_id)
            mysqlConn.cursor().execute(
                "INSERT INTO qrcode_table (data_id, equ_id, link, image) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE link=%s, image=%s",
                (data_id,
                 equ_id,
                 url,
                 imageData,
                 url,
                 imageData))
            mysqlConn.commit()
            return (url, r.json()['data']['front_text'],
                    r.json()['data']['behind_text'])
        else:
            logger.error(
                "request failed! status_code={}".format(
                    r.status_code))
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
    if res:
        return left
    else:
        return right


def job():
    logger.info("start job!!!")
    for row in getNewRowsFromUpLog()[:1]:  # 每个周期只处理一个
        port = row[2]
        if port == 4000:
            logger.info("skip port = 4000!")
            updateIsNewFromUpLog(row)
            return
        result = doGetRequest(row)
        if result:
            updateIsNewFromUpLog(row)
            #doWriteDownLog(row, result[1], result[2])
            time.sleep(3)
            handleMysqlStatus(row)
    logger.info("finish job!!!")

#import schedule
# schedule.every(2).seconds.do(job)

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket


class QRCodeServerSvc(win32serviceutil.ServiceFramework):
    _svc_name_ = "QRCodeService"
    _svc_display_name_ = "QR Code Service"
    _svc_description_ = "This service is used for QR Code handling."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

    def main(self):
        while True:
            schedule.run_pending()
            # job()
            time.sleep(1)

if __name__ == '__main__':
    while True:
        # schedule.run_pending()
        job()
        time.sleep(1)
    # win32serviceutil.HandleCommandLine(QRCodeServerSvc)
