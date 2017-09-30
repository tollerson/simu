#encoding=utf-8
import math
import numpy as np
import pandas as pd
from datetime import *
import os
import pymysql
from sqlalchemy import create_engine
from WindPy import w

#w.start()
#w.start(waitTime=60)

def sqldb():
    myEngine = create_engine("mysql+pymysql://taoniu:"+'taoniu'+"@192.168.10.121/stock?charset=utf8", encoding="utf-8")
    myConnect = myEngine.connect()
    df = pd.read_sql_query("SELECT fundId FROM stock.simuwang_fund_data WHERE Date(Date)>'2017-08-01';", myEngine)
    df0 = df.drop_duplicates()
    #df0.to_excel('fundID08.xlsx', startrow=5)
    #print dfnew.head()
    print df0.head(10)
    print df0.tail(10)
    print 'numbers of funds: ', len(df0)
    print 'numbers without drop duplicates: ', len(df)
    
def idact():
    pf = pd.read_excel('fundID08.xlsx', skiprows=5) # , names=['Date', 'CPI-China']   
    return pf
    
def com(ty='RB1709.SHF', t='2017-01-01'):
    M = w.wsd(ty, "close", t, datetime.today(), "Period=D")
    mdf = pd.DataFrame(M.Data, index=[ty], columns=M.Times)
    mdf = mdf.T
    print ty
    print mdf.head(10)
    print mdf.tail(10)  
    return mdf
    
'''    
def doing():
    rbdf = com()
    myEngine = create_engine("mysql+pymysql://taoniu:"+'taoniu'+"@192.168.10.121/stock?charset=utf8", encoding="utf-8")
    myConnect = myEngine.connect()
    for id in idact()['fundId'][:20]:
        df = pd.read_sql_query("SELECT fundId Date NewWorthDelta FROM stock.simuwang_fund_data WHERE (Date(Date)>'2017-01-01') AND (fundId = id);", myEngine)
        if len(df) < 6:
            continue
        else:
            for ld in df['Date']:
                #find ld in rbdf.index blabla
'''

def doing():
    myEngine = create_engine("mysql+pymysql://taoniu:"+'taoniu'+"@192.168.10.121/stock?charset=utf8", encoding="utf-8")
    myConnect = myEngine.connect()
    dbig = pd.DataFrame(columns = ['fundId', 'Date', 'NetWorth', 'NewWorthDelta'])
    for id in idact()['fundId'][:10]:
        print id, 'is ongoing......'
        df = pd.read_sql_query("SELECT fundId Date NetWorth NewWorthDelta FROM stock.simuwang_fund_data WHERE (Date(Date)>'2017-01-01') AND (fundId = id);", myEngine)
        if len(df) < 6:
            continue
        else:
            dbig = pd.merge(dbig, df, how='outer')
        print id, 'is well done......'
    dbig.to_excel('simufund.xlsx', skiprows=5)

                
if __name__ == "__main__":
    #doing()
    sqldb()
    #idact()
    #com()
    print 'Good!'
    
    
    
    
# database of songpeng xu, ID: taoniu, KEY: taoniu, IP: 192.168.10.121
# simupaipaiwang, ID: 13906805168, KEY: cdswjhbkdj1
# datatable, Name: stock.simuwang_fund_data, List of Keys: fundId Date NetWorth AccNetWorthReinvest AccNetWorthNoReinvest NewWorthDelta
# 基金ID 日期 单位净值 累计净值(分红再投资) 累计净值(分红不投资) 净值变动