#!/usr/bin/python
# -*- coding=utf8 -*-
# Description: Loading weeking day for cp
# Author: Shaohui Dong
import csv
import DB
import datetime

def Connent_Online_Mysql_By_DB(hostname,port,username,pwd,dbname,socket):
    db = DB.DB(False,host=hostname, port=port, user=username ,passwd=pwd, db=dbname,charset='gbk', unix_socket=socket) 
    return db

# record db
def write_record_db(db,list_obj,table_name):
    try:
        db.insert(table_name,list_obj)
        db.commit()
    except Exception,e:
        print e

def loading_weekday_func():
	db = Connent_Online_Mysql_By_DB('rdseq3ujqveuq6f.mysql.rds.aliyuncs.com',3306,'p2p_ceshi','AAaa1234','p2p','/tmp/mysql.sock')
	print db
	f = open('../data/workingdate.csv','r')
	rc = f.readlines()
	rc = map(lambda x:x.strip(),rc)
	for i in range(1,len(rc)):
		working_info = {}
		line = rc[i]
		working_t = line.split(',')
		year = (int)(working_t[0].split('/')[0])
		month = (int)(working_t[0].split('/')[1])
		day = (int)(working_t[0].split('/')[2])
		working_info['DATE'] = datetime.date(year,month,day).strftime("%Y-%m-%d")

		working_info['IS_WEEKDAY'] = working_t[1]
		print working_info 
		#working_info[a]
		write_record_db(db,working_info,'cp_withdraw_weekday')
 
	

if __name__ == '__main__':
	loading_weekday_func()

