#!/usr/bin/python
#-*- coding:utf-8 -*-

import datetime
import MySQLdb
import os 

##把 bmsglog 表180天前的数据归档到 bmsglog_archive 表，需求from lam、丰圣谋 20170120##

dbuser='user'
dbuserpass='pass'

dbmmsuser='user'
dbmmsuserpass='pass'

print "start archive table data BEGIN:"
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="insert into bmsglog_archive select * from bmsglog where addtime < date_sub(curdate(), interval 180 day);"
sql2="delete from bmsglog where addtime <  date_sub(curdate(), interval 180 day);"

try:
   cursor.execute(sql1) 
   print "table bmsglog_archive insert:",cursor.rowcount
   cursor.execute(sql2)
   print "table bmsglog delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

##广点通只接收5天内的有效点击数据上传，部分旧数据堆积在表里影响性能，必须做定期清理 需求from lam   BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="insert into promo_data_submit_detail_archive select * from promo_data_submit_detail where status=0 and active_time < date_sub(curdate(), interval 14 day);"
sql2="delete from promo_data_submit_detail where status=0 and active_time < date_sub(curdate(), interval 14 day);"

try:
   cursor.execute(sql1) 
   print "table promo_data_submit_detail_archive insert:",cursor.rowcount
   cursor.execute(sql2)
   print "table promo_data_submit_detail delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

##支付宝回调数据自动归档,自动归档规则, 按字段: notify_time, 只保留1年内记录，其它的记录转表归档表 payment_ali_archive 需求from lam   BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="insert into payment_ali_archive select * from payment_ali where notify_time < date_sub(curdate(), interval 365 day) and notify_time > '0000-00-00 00:00:00';"
sql2="delete from payment_ali where notify_time < date_sub(curdate(), interval 365 day) and notify_time > '0000-00-00 00:00:00';"

try:
   cursor.execute(sql1) 
   print "table payment_ali_archive insert:",cursor.rowcount
   cursor.execute(sql2)
   print "table payment_ali delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

##定期清理IP黑名单, 按字段 update_time, 只保留1个月内记录，其它的记录转表归档表 pywsdk_black_ip_archive 需求from 谭燕仪   20180628 BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="insert into pywsdk_black_ip_archive select * from pywsdk_black_ip where update_time < date_sub(curdate(),interval 1 month);"
sql2="delete from pywsdk_black_ip where update_time < date_sub(curdate(),interval 1 month);"

try:
   cursor.execute(sql1) 
   print "table pywsdk_black_ip_archive insert:",cursor.rowcount
   cursor.execute(sql2)
   print "table pywsdk_black_ip delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

##定时清理处理过程产生的临时表，规则如下：1. 临时表建立1周之后删除 2.临时表名规则：msisdn_tag_temp_<num> 例：msisdn_tag_temp_92 需求from LAM  BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbmmsuser,passwd=dbmmsuserpass,db="xiaopeng2_mmspromo",port=3307)
cursor = db.cursor()
sql1="select concat('drop table ',TABLE_NAME,';') from information_schema.tables where TABLE_SCHEMA='xiaopeng2_mmspromo' and TABLE_NAME REGEXP 'msisdn_tag_temp_[0-9]' and  CREATE_TIME < date_sub(curdate
(),interval 7 day) into outfile '/proj/logs/mms_process_file/drop_table.txt';"

curdate=datetime.datetime.now().strftime('%Y-%m-%d')
file='/proj/logs/mms_process_file/drop_table.txt'

if os.path.exists(file):
    bakfile=file + '_' + curdate + '.bak'
    os.rename(file,bakfile)

cursor.execute(sql1)

if os.path.getsize(file):
    f=open(file,'r')
    lines = f.readlines()
    for fileline in lines:
        sql2=fileline
        cursor.execute(sql2)
        print fileline
    f.close()
else:
    print "msisdn_tag_temp_<num> not template table to drop!"

db.close()

filename=os.path.splitext(file)[0]
filetype=os.path.splitext(file)[1]
newfile=filename + '_' + curdate + filetype
os.rename(file,newfile)

##xiaopeng2.pywsdk_cache_account 定时清理 清理规则：1. update time在180天之前 2. 直接删除 需求from LAM   20180727 BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="delete from pywsdk_cache_account where update_time < date_sub(curdate(),interval 180 day);"

try:
   cursor.execute(sql1) 
   print "table pywsdk_cache_account delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

##对发行游戏聊天数据表进行自动归档，按字段：create_time，只保留该表中7天的数据，历史数据自动进入归档表备用, 需求from 谭燕仪 20180802 BEGIN

db = MySQLdb.connect(host="127.0.0.1",user=dbuser,passwd=dbuserpass,db="xiaopeng2")
cursor = db.cursor()
sql1="insert into pywsdk_chat_record_archive select * from pywsdk_chat_record where create_time < date_sub(curdate(),interval 7 day);"
sql2="delete from pywsdk_chat_record where create_time < date_sub(curdate(),interval 7 day);"

try:
   cursor.execute(sql1) 
   print "table pywsdk_chat_record_archive insert:",cursor.rowcount
   cursor.execute(sql2)
   print "table pywsdk_chat_record delete:",cursor.rowcount
   db.commit()
except:
   db.rollback()
db.close()

print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print "END archive table data"
