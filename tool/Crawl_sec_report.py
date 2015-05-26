#!/usr/bin/python
# -*- coding=utf8 -*-

from BeautifulSoup import BeautifulSoup
import sys,os
import urllib2
import csv
import threading


def multi_process(thread_num):
    thread_list = list(); 
    stock_code_list = fetch_all_stock_code_from_conf()
    slide_len = len(stock_code_list) / thread_num
    start = 0
    end = 0
    for index in range(0,thread_num):
        start = end
        end = end + slide_len
        if end >= len(stock_code_list):
            end = len(stock_code_list)
        thread_name = "thread_%s" %index
        # fetch_all_sec_link(stock_code_list[start:end])    
        thread_list.append(threading.Thread(target = fetch_all_sec_link, name = thread_name, args = (stock_code_list[start:end],)))
    
    for thread in thread_list: 
        thread.start()
    
    for thread in thread_list:
        thread.join()

def Connent_Online_Mysql_By_DB(hostname,port,username,pwd,dbname,socket):
    db = DB.DB(False,host=hostname, port=port, user=username ,passwd=pwd, db=dbname,charset='gbk', unix_socket=socket) 
    return db


def fetch_all_stock_code(db):
    fetch_code_sql = "select distinct(stockcode) from us_stock_income_year_table"
    fetch_code_result = db.select(fetch_code_sql)
    wf = open('../conf/all_stockcode.conf','w')
    for line in fetch_code_result:
        wf.write(line[0] + '\n')
    wf.close()
    # return fetch_code_result


def fetch_all_stock_code_from_conf():
    f = open('../conf/all_stockcode.conf','r')
    rc = f.readlines()
    rc = map(lambda x: x.strip(), rc)
    f.close()
    return rc

def fetch_all_report(threading_num):
    stock_code_list = fetch_all_stock_code_from_conf()
    slide_len = len(stock_code_list) / threading_num
    start = 0
    end = 0
    for index in range(0,threading_num):
        start = end
        end = end + slide_len
        if end >= len(stock_code_list):
            end = len(stock_code_list)
        fetch_all_sec_link(stock_code_list[start:end])
        


def fetch_all_sec_link(stock_code_list):
    # stock_code_list = fetch_all_stock_code_from_conf()
    for stock_code in stock_code_list:
        origin_code = stock_code[0]
        try:
            os.mkdir('../data/' + origin_code)
        except Exception as e:
            print "mkdir error in a"
            
        sec_report_link = "http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=" + origin_code + "&type=&dateb=&owner=exclude&count=100"
        try:
            content = urllib2.urlopen(sec_report_link).read()
            soup = BeautifulSoup(content)
            all_docs = soup.findAll('tr')
            for doc_tag in all_docs:
                try:
                    doc_name = doc_tag.findAll("td")[0].string
                    print doc_name
                    if doc_name == '10-K' or doc_name == '10-k':
                        doc_link = doc_tag.findAll("td")[1].find('a')['href']
                        sec_10_k_file_name = "http://www.sec.gov" + doc_link
                        date_str = doc_tag.findAll("td")[3].string
                        print "hi 10-K"
                        download_report_file(origin_code,sec_10_k_file_name,'10-K',date_str)
                    elif doc_name == '10-Q' or doc_name == '10-q':
                        doc_link = doc_tag.findAll("td")[1].find('a')['href']
                        sec_10_q_file_name = "http://www.sec.gov" + doc_link
                        date_str = doc_tag.findAll("td")[3].string
                        print "hi 10-Q"
                        download_report_file(origin_code,sec_10_q_file_name,'10-Q',date_str)
                    elif doc_name == '20-F' or doc_name == '20-f':
                        doc_link = doc_tag.findAll("td")[1].find('a')['href']
                        sec_20_f_file_name = "http://www.sec.gov" + doc_link
                        date_str = doc_tag.findAll("td")[3].string
                        print "hi 20-F"
                        download_report_file(origin_code,sec_20_f_file_name,'20-F',date_str)
                    elif doc_name == '6-K' or doc_name == '6-k':
                        # 6-k文件单独处理
                        doc_link = doc_tag.findAll("td")[1].find('a')['href']
                        sec_6_k_file_name = "http://www.sec.gov" + doc_link
                        date_str = doc_tag.findAll("td")[3].string
                        print "hi 6-K"
                        download_report_file(origin_code,sec_6_k_file_name,'6-K',date_str)
                except Exception as e:
                    print "error"
                    
        except Exception as e:
            print "exception !"

def download_report_file(code,report_link,report_type,report_date):
    try:
        content = urllib2.urlopen(report_link).read()
        soup = BeautifulSoup(content)
        table_tag = soup.findAll("table",{'class':'tableFile','summary':'Document Format Files'})
        if table_tag == None or len(table_tag)!= 1:
            return
        all_rows = table_tag[0].findAll('tr')
        for row_tag in all_rows:
            tds = row_tag.findAll('td')
            try:
                if report_type in tds[3].string:
                    rst_link = 'http://www.sec.gov' + tds[2].find('a')['href']
                    # mkdir
                    try:
                        os.mkdir('../data/' + code + '/' + report_type)
                    except Exception as e:
                        print 'mkdir error'
                    
                    try:
                        os.mkdir('../data/' + code + '/' + report_type + '/' + report_date)
                    except Exception as e:
                        print 'mkdir error'
                    
                    try:
                        os.system('wget -q ' + rst_link + ' -t 5 -O ' + '../data/' + code + '/' + report_type + '/' + report_date + '/' + code + '_' + report_type + '_' + report_date + '_report.htm')
                    except Exception as e:
                        print 'wget file error'
                    
            except Exception as e:
                continue
    except Exception as e:
        return

    
if __name__ == '__main__':
    # db = Connent_Online_Mysql_By_DB('jsfundcf.mysql.rds.aliyuncs.com',3306,'jsfundcf','AAaa1234','js_fund','/tmp/mysql.sock')
    # fetch_all_stock_code(db)
    multi_process(10)
