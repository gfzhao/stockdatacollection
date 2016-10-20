#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
import math
import logging
import tushare as ts
import pandas as pd
import numpy as np
import datetime
from sqlalchemy.dialects import mysql
from model.mysqlOrmMapper import *
from lib.mysqlHandle import *
mysqlEngine =  MysqlHandle.engine

#CREATE DATABASE stocks_information DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
#UPDATE user SET password=PASSWORD('zhangting') WHERE user='root';
#FLUSH PRIVILEGES;

class StockForecastData:
   def get_stockForecastData(self,year,quarter):
      try:
         if quarter == 2 or quarter == 3:
            date = '%s-%s-%s' % (str(year),str(3*quarter),'30')
         else:
            date = '%s-%s-%s' % (str(year),str(3*quarter),'31')
         print '-------------------'
         print date
         print '-----------------------'
         df = ts.forecast_data(year,quarter)
         df['date'] = np.array([date for i in range(len(df))])
         f_ranges = list(df['range'])
         min_range = []
         max_range = []
         index = 0
         nanIdxs = []
         print f_ranges
         for f_range in f_ranges:
            index = index+1
            if f_range == '0':
               min_range.append(0)
               max_range.append(0)
               continue
            try:
               x = float(f_range)
               if math.isnan(float(f_range)) != True:
                  min_range.append(x*100)
                  max_range.append(x*100)
                  continue
            except:
               ranges = f_range.split('~')
               if len(ranges) == 2:
                  min_range.append(float(ranges[0].strip('%')))
                  max_range.append(float(ranges[1].strip('%')))
               else:
                  min_range.append(float(ranges[0].strip('%')))
                  max_range.append(float(ranges[0].strip('%')))
               continue
            if math.isnan(float(f_range)) == True:
               nanIdxs.append(index-1)
         for nanIdx in nanIdxs:
            df['date'].pop(nanIdx)
            df['code'].pop(nanIdx)
            df['report_date'].pop(nanIdx)
            df['type'].pop(nanIdx)
            df['pre_eps'].pop(nanIdx)
         new_codes = []
         codes = list(df['code'])
         idx = 0
         dupIdxs = []
         for code in codes:
            if code not in new_codes:
               new_codes.append(code)
            else:
               dupIdxs.append(idx)
            idx = idx+1
         for dupIdx in dupIdxs:
            df['date'].pop(dupIdx)
            df['report_date'].pop(dupIdx)
            df['type'].pop(dupIdx)
            df['pre_eps'].pop(dupIdx)
            min_range.pop(dupIdx)
            max_range.pop(dupIdx)
         date = np.array(df['date'])
         code = np.array(new_codes)
         report_date = np.array(df['report_date'])
         forecast_type = np.array(df['type'])
         pre_eps = np.array(df['pre_eps'])
         min_range = np.array(min_range)
         max_range = np.array(max_range)
         arrays = [date,code]
         tuples = list(zip(*arrays))
         index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
         s = pd.Series(forecast_type, index=index)
         f = s.to_frame()
         f.rename(index=str, columns={0: "forecast_type"},inplace=True)
         f['report_date'] = report_date
         f['pre_eps'] = pre_eps
         f['min_range'] = min_range
         f['max_range'] = max_range
         f.to_sql('stock_forecast_data',mysqlEngine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
         """
         mh = MysqlHandle()
         DFToSQLOneByOne(mh.session,f,'Report')
         """
      except  Exception, e:
         print e

if __name__ == "__main__":
   sr = StockForecastData()
   sr.get_stockForecastData(2016,3)
