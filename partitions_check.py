#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-07-26
# @Author  : pinocao
import datetime
import logging
import json
import urllib.request
import pymysql
'''
检测分区情况
可用分区小于一个月自动增加一个月的分区
每周检测，通过钉钉提醒
并删除两周前的历史分区
'''


TabletoCheck = {'testdb': ['t1', 't2', 't3']}  # 需要检测的库、表
dbuser = 'test'
dbpass = 'test'
dbhost = '127.0.0.1'
dbport = 3306
WorkPath = "./"    # 日志目录
today = datetime.datetime.now()
two_week_ago = (datetime.datetime.now() - datetime.timedelta(weeks=2)).strftime('%Y-%m-%d %H:%M:%S')


# 日志记录
def createlog():
    logname = WorkPath + 'insert.log'
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logname,
                        filemode='a')
    logger = logging.getLogger('execute_logger')
    return logger


log = createlog()

'''
python3版本将urllib2目录与urllib合并了
其中urllib2.urlopen()变成了urllib.request.urlopen()
urllib2.Request()变成了urllib.request.Request() 
'''
# 钉钉机器人提醒
def sendDD(Msg):
        url = ""   # 钉钉机器人url

#       构建请求头部

        header = {
            "Content-Type":"application/json",
            "Charset": "utf-8"
        }

#       构建请求数据
        data = {
            "msgtype": "text",
            "text": {
                "content": Msg
            },

            "at": {
                "isAtAll": False
            }
        }

#       对请求数据进行json封装，转换为json格式
        sendData = json.dumps(data).encode(encoding="utf-8")


#       发送请求

        request = urllib.request.Request(url=url,data=sendData,headers= header)


#       将返回的请求构建成文件格式
        opener = urllib.request.urlopen(request)


# 查询最大分区
def select_last(table_schema,table_name):
    try:
        db = pymysql.connect(host=dbhost, port=dbport, user=dbuser, passwd=dbpass, db="information_schema")
        cursor = db.cursor()
        select_cmd = '''select from_unixtime(PARTITION_DESCRIPTION)  from partitions  \
where table_schema='%s' and table_name='%s' order by PARTITION_DESCRIPTION DESC limit 1 ;''' %(table_schema, table_name)
        cursor.execute(select_cmd)
        result = cursor.fetchall()
        lasttime = result[0][0].strftime('%Y-%m-%d %H:%M:%S')
        log.info('%s %s ' %(table_schema, table_name) + ' last partition is: ' + lasttime)
        return lasttime
    except Exception as e:
        log.error(e)
        return e
    finally:
        db.close()


# 查询两周前的历史分区
def select_history(table_schema, table_name):
    try:
        db = pymysql.connect(host=dbhost, port=dbport, user=dbuser, passwd=dbpass, db="information_schema")
        cursor = db.cursor()
        select_cmd = '''select partition_name  from partitions  \
where table_schema='%s' and table_name='%s' AND PARTITION_DESCRIPTION < unix_timestamp('%s');''' %(table_schema, table_name, two_week_ago)
        cursor.execute(select_cmd)
        result = cursor.fetchall()
        history_list = []
        for row in result:
            history_list.append(row[0])
        return history_list
    except Exception as e:
        log.error(e)
        return e
    finally:
        db.close()


# 执行sql
def execute(cmd, table_schema):
    try:
        db = pymysql.connect(host=dbhost, port=dbport, user=dbuser, passwd=dbpass, db=table_schema)
        cursor = db.cursor()
        cursor.execute(cmd)
        effect_rows = str(cursor.rowcount)
        db.commit()
        log.info('Successful ' + 'effect_rows:' + effect_rows + ' ' + cmd )
        return "Successful"
    except Exception as e:
        db.rollback()
        log.error(e)
        return e
    finally:
        db.close()


log.info("------------" + 'START' + "------------")
startrecord = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

drop_message_list = []
add_message_list = []

for db in TabletoCheck.keys():
    for tb in TabletoCheck[db]:
        res = select_last(db, tb)
        history_list = select_history(db, tb)
        if history_list:
            for history_partition in history_list:
                drop_partition_sql = '''alter table %s drop partition %s;''' %(tb, history_partition)
                execute(drop_partition_sql,db)
            drop_message = "%s 正在删除两周前历史分区..."%tb
            drop_message_list.append(drop_message)
        else:
            drop_message = "%s 无需删除历史分区" % tb
            drop_message_list.append(drop_message)
        if (datetime.datetime.strptime(res, '%Y-%m-%d %H:%M:%S') - today).days > 30:
            add_message = "%s 分区大于一个月，无需新建分区"%tb
            add_message_list.append(add_message)
        else:
            add_message = "%s 分区小于一个月，新建分区中..."%tb
            add_message_list.append(add_message)
            for i in range(4):
                nexttime = datetime.datetime.strptime(res, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(weeks=i+1)

                add_partition_name = 'p' + nexttime.strftime('%Y%m%d')
                add_partition_sql = '''alter table %s add partition (partition %s VALUES LESS THAN (unix_timestamp('%s')) ENGINE = InnoDB);'''\
                                %(tb, add_partition_name, nexttime.strftime('%Y-%m-%d %H:%M:%S'))
                execute(add_partition_sql,db)
sendDD('\n'.join(drop_message_list) + '\n' + '\n'.join(add_message_list))

log.info("------------" + ' END ' + "------------")
endrecord = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
endmessage = "分区检测/操作完成"

sendDD(endmessage + '\n开始时间：' + startrecord + '\n结束时间：' + endrecord)
