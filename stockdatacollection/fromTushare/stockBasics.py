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

class StockBasics:
   #
   # public:
   # Get the stock basic information from tushare, such as:
   # stock code, stock name, time to market,etc.
   #
   def get_stock_basics(self):
      try:
         df = ts.get_stock_basics()
         #df.reset_index(level=0,inplace=True)
         df.to_sql('stock_basics',mysqlEngine,if_exists='replace',
                  index=True,index_label='code',dtype={'code':mysql.NVARCHAR(6)})
      except  Exception, e:
         print e

if __name__ == "__main__":
   sb = StockBasics()
   sb.get_stock_basics()
