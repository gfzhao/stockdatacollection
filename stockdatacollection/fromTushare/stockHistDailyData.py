import os,sys
import logging
import tushare as ts
import pandas as pd
import numpy as np
import datetime
from sqlalchemy.dialects import mysql

from lib.mysqlHandle import *
mysqlEngine =  MysqlHandle.engine

#CREATE DATABASE stocks_information DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
#UPDATE user SET password=PASSWORD('zhangting') WHERE user='root';
#FLUSH PRIVILEGES;
nowYear = datetime.datetime.now().year
nowMonth = datetime.datetime.now().month
nowDay = datetime.datetime.now().day

if nowMonth < 10:
   nowMonth = '0%s' % nowMonth

if nowDay < 10:
   nowDay = '0%s' % nowDay

riQi = "%s-%s-%s" % (nowYear,nowMonth,nowDay)

class StockHistDailyData:
   #
   # public:
   # Get the stock history daily trade data,such as open,high,low,close,etc.
   #
   def get_hist_daily_trade_data(self,code,startDate,endDate):
      try:
         df = ts.get_h_data(code,startDate,endDate)
         df.reset_index(level=0,inplace=True)
         df['code'] = [code for i in range(len(df))]
         date = np.array(df['date'])
         code = np.array(df['code'])
         open = np.array(df['open'])
         high = np.array(df['high'])
         close = np.array(df['close'])
         low = np.array(df['low'])
         volume = np.array(df['volume'])
         amount = np.array(df['amount'])
         arrays = [date,code]
         tuples = list(zip(*arrays))
         index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
         s = pd.Series(open, index=index)
         f = s.to_frame()
         f.rename(index=str, columns={0: "open"},inplace=True)
         f['high'] = high
         f['close'] = close
         f['low'] = low
         f['volume'] = volume
         f.to_sql('stock_daily_kt_data',StockHelper.engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
         #stocklib.DFToSQLOneByOne(self.session,f,'StockDailyKT')
      except  Exception, e:
         print e

   def get_today_daily_trade_data(self):
      try:
         df = ts.get_today_all()
         df['date'] = [riQi for i in range(len(df))]
         date = np.array(df['date'])
         code = np.array(df['code'])
         open = np.array(df['open'])
         high = np.array(df['high'])
         close = np.array(df['trade'])
         low = np.array(df['low'])
         volume = np.array(df['volume'])
         arrays = [date,code]
         tuples = list(zip(*arrays))
         index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
         s = pd.Series(open, index=index)
         f = s.to_frame()
         f.rename(index=str, columns={0: "open"},inplace=True)
         f['high'] = high
         f['close'] = close
         f['low'] = low
         f['volume'] = volume
         """
         f.to_sql('stock_daily_kt_data',StockHelper.engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
         """
         stocklib.DFToSQLOneByOne(self.session,f,'StockDailyKT')
      except  Exception, e:
         print e

if __name__ == "__main__":
   shdd = StockHistDailyData()
   shdd.get_hist_daily_trade_data()
