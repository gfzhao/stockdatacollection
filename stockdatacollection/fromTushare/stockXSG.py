#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
import logging
import tushare as ts
import pandas as pd
import numpy as np
import datetime
from sqlalchemy.dialects import mysql
from model.mysqlOrmMapper import *
from lib.mysqlHandle import *
mysqlEngine =  MysqlHandle.engine

logging.basicConfig(filename='stockTushare.log',level=logging.WARN)

#CREATE DATABASE stocks_information DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
#UPDATE user SET password=PASSWORD('zhangting') WHERE user='root';
#FLUSH PRIVILEGES;

class StockXSG:
   def get_xsg_monthly(self):
      try:
         df = ts.xsg_data()
         df.drop_duplicates(inplace=True)
         date = np.array(df['date'])
         code = np.array(df['code'])
         count = np.array(df['count'])
         ratio = np.array(df['ratio'])
         arrays = [date,code]
         tuples = list(zip(*arrays))
         index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
         s = pd.Series(count, index=index)
         f = s.to_frame()
         f.rename(index=str, columns={0: "count"},inplace=True)
         f['ratio'] = ratio
         f.to_sql('stock_xsg_data',mysqlEngine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
         """
         mh = MysqlHandle()
         DFToSQLOneByOne(mh.session,f,'Report')
         """
      except  Exception, e:
         print e

if __name__ == "__main__":
   sr = StockXSG()
   sr.get_xsg_monthly()
