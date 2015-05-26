#!/usr/bin/python
# -*- coding=utf8 -*-
# Author : Dong Shaohui
# Description: Crawl US Stock Balance Quarter Sheet Data, And Load The Data Into Database
# Mail : dongsh@jsfund.cn
# Date : 2014-11-17

from BeautifulSoup import BeautifulSoup
import sys,os
import DB
import urllib2
import csv

g_total = 0
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


def crawl_stock_us_url(stock_code):
    global g_total
    data_file_name = "../data/balance_sheet_quarter_report/" + stock_code  + "_quarter_report.csv"
    single_stock_url = "http://quotes.money.163.com/usstock/"+ stock_code + "_balance.html?type=quarter"
    try:
        content = urllib2.urlopen(single_stock_url).read()
        soup = BeautifulSoup(content)
    except:
        g_total += 1
        # print g_total
        return
    # 文字描述部分爬取
    title_desc_info = soup.find('div',{'class':'list_title'})
    try:
        date_desc_tag = title_desc_info.find('span')
    except:    
        g_total += 1
        # print g_total
        return
        
    _date_desc_context = date_desc_tag.string # 1 
    
    all_balance_title_info_tags = title_desc_info.findAll('li')

    # 数据部分爬取
    data_table_info = soup.find('div',{'id':'list_table'})
    _date_list = data_table_info.find('ul').findAll('div')
    data_table_info = data_table_info.find('table')
    _data_info_detail_tags = data_table_info.findAll('tr')
    
    
    print single_stock_url
    f = open(data_file_name,'w')
    
    #日期描述
    date_desc_str = date_desc_tag.string+";"
 
    
    for _data_info_detail_tag in _date_list:
        date_desc_str = date_desc_str + _data_info_detail_tag.string + ";"
    date_desc_str = date_desc_str[:-1]
    f.write(date_desc_str.encode('gbk')+"\n")
    

    
    for index in range(1,len(all_balance_title_info_tags)):
        td_tags_info = _data_info_detail_tags[index-1].findAll('td')
        td_info_detail = ""
        for td_tag_info in td_tags_info:
            if td_tag_info.string == None:
                continue
            td_info_detail = td_info_detail + td_tag_info.string + ";"
        if td_info_detail != "":
            td_info_detail = all_balance_title_info_tags[index].string + ";" + td_info_detail[:-1]
        else:
            td_info_detail = all_balance_title_info_tags[index].string
        f.write(td_info_detail.encode('gbk')+"\n")
    f.close()
    
    
    

def fetch_all_us_stock_code(db):
    fetch_code_sql = "select stockcode from all_us_stock_info"
    fetch_code_result = db.select(fetch_code_sql)
    for code_tuple in fetch_code_result:
        crawl_stock_us_url(code_tuple[0])


if __name__ == '__main__':
    # reload(sys)
    db = Connent_Online_Mysql_By_DB('jsfundb0qu.mysql.rds.aliyuncs.com',3306,'market_7778','AAaa1234','usstock_report','/tmp/mysql.sock')
    fetch_all_us_stock_code(db)
