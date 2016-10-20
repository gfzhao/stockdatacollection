#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys,os
import logging
from sqlalchemy.orm import mapper
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base

from lib.mysqlHandle import *
database_engine =  MysqlHandle.engine

Base = declarative_base(database_engine)

class StockDayKdjMacd(Base):
   __tablename__ = 'day_kdj_macd'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   open_p = Column('open',mysql.DOUBLE)
   high_p = Column('high',mysql.DOUBLE)
   low_p = Column('low',mysql.DOUBLE)
   close_p = Column('close',mysql.DOUBLE)
   dif = Column('dif',mysql.DOUBLE)
   dea = Column('dea',mysql.DOUBLE)
   macd = Column('macd',mysql.DOUBLE)
   k = Column('k',mysql.DOUBLE)
   d = Column('d',mysql.DOUBLE)
   j = Column('j',mysql.DOUBLE)
   j_chaomai_flag = Column('j_chaomai_flag',mysql.INTEGER)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.open = kargs['open']
        self.high = kargs['high']
        self.low = kargs['low']
        self.close = kargs['close']
        self.dif = kargs['dif']
        self.dea = kargs['dea']
        self.macd = kargs['macd']
        self.k = kargs['k']
        self.d = kargs['d']
        self.j = kargs['j']
        self.j_chaomai_flag = kargs['j_chaomai_flag']

class StockWeekKdjMacd(Base):
   __tablename__ = 'week_kdj_macd'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   open_p = Column('open',mysql.DOUBLE)
   high_p = Column('high',mysql.DOUBLE)
   low_p = Column('low',mysql.DOUBLE)
   close_p = Column('close',mysql.DOUBLE)
   dif = Column('dif',mysql.DOUBLE)
   dea = Column('dea',mysql.DOUBLE)
   macd = Column('macd',mysql.DOUBLE)
   k = Column('k',mysql.DOUBLE)
   d = Column('d',mysql.DOUBLE)
   j = Column('j',mysql.DOUBLE)
   j_chaomai_flag = Column('j_chaomai_flag',mysql.INTEGER)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.open = kargs['open']
        self.high = kargs['high']
        self.low = kargs['low']
        self.close = kargs['close']
        self.dif = kargs['dif']
        self.dea = kargs['dea']
        self.macd = kargs['macd']
        self.k = kargs['k']
        self.d = kargs['d']
        self.j = kargs['j']
        self.j_chaomai_flag = kargs['j_chaomai_flag']

class StockMonthKdjMacd(Base):
   __tablename__ = 'month_kdj_macd'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   open_p = Column('open',mysql.DOUBLE)
   high_p = Column('high',mysql.DOUBLE)
   low_p = Column('low',mysql.DOUBLE)
   close_p = Column('close',mysql.DOUBLE)
   dif = Column('dif',mysql.DOUBLE)
   dea = Column('dea',mysql.DOUBLE)
   macd = Column('macd',mysql.DOUBLE)
   k = Column('k',mysql.DOUBLE)
   d = Column('d',mysql.DOUBLE)
   j = Column('j',mysql.DOUBLE)
   j_chaomai_flag = Column('j_chaomai_flag',mysql.INTEGER)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.open = kargs['open']
        self.high = kargs['high']
        self.low = kargs['low']
        self.close = kargs['close']
        self.dif = kargs['dif']
        self.dea = kargs['dea']
        self.macd = kargs['macd']
        self.k = kargs['k']
        self.d = kargs['d']
        self.j = kargs['j']
        self.j_chaomai_flag = kargs['j_chaomai_flag']

Base.metadata.create_all(database_engine)

if __name__ == "__main__":
   mh = MysqlHandle()
   session = mh.session
   #res = session.query(StockBasic).all()
   res = session.query(StockBasic).all()
   index = 0
   for item in res:
      index += 1
      print item.code,item.name
   print index
