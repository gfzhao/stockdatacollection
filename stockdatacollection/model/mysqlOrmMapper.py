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

class StockBasic(Base):
   __tablename__ = 'stock_basics'
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   name = Column('name',mysql.TEXT)
   industry = Column('industry',mysql.TEXT)
   area = Column('area',mysql.TEXT)
   pe = Column('pe',mysql.DOUBLE)
   outstanding = Column('outstanding',mysql.DOUBLE)
   totals = Column('totals',mysql.DOUBLE)
   totalAssets = Column('totalAssets',mysql.DOUBLE)
   liquidAssets = Column('liquidAssets',mysql.DOUBLE)
   fixedAssets = Column('fixedAssets',mysql.DOUBLE)
   reserved = Column('reserved',mysql.DOUBLE)
   reservedPerShare = Column('reservedPerShare',mysql.DOUBLE)
   esp = Column('esp',mysql.DOUBLE)
   bvps = Column('bvps',mysql.DOUBLE)
   pb = Column('pb',mysql.DOUBLE)
   timeToMarket = Column('timeToMarket',mysql.BIGINT(20))
   def __init__(self, code, name, industry, area,
                 pe, outstanding, totals, totalAssets, liquidAssets,
                 fixedAssets, reserved, reservedPerShar, esp, bvps, pb, timeToMarket):
        
        self.code = code
        self.name = name
        self.industry = industry
        self.area = area
        self.pe = pe
        self.outstanding = outstanding
        self.totals = totals
        self.totalAssets = totalAssets
        self.liquidAssets = liquidAssets
        self.fixedAssets = fixedAssets
        self.reserved = reserved
        self.reservedPerShar = reservedPerShar
        self.esp = esp
        self.bvps = bvps
        self.pb = pb
        self.timeToMarket = timeToMarket

class StockDailyKT(Base):
   """
   Stock daily KT class
   """
   __tablename__ = 'stock_daily_kt_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   open = Column('open',mysql.DOUBLE)
   high = Column('high',mysql.DOUBLE)
   close = Column('close',mysql.DOUBLE)
   low = Column('low',mysql.DOUBLE)
   volume = Column('volume',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.open = kargs['open']
        self.high = kargs['high']
        self.close = kargs['close']
        self.low = kargs['low']
        self.volume = kargs['volume']

class FinanceData(Base):
   """
   Stock Report class
   """
   __tablename__ = 'stock_finance_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   eps = Column('eps',mysql.DOUBLE)
   net_profits = Column('net_profits',mysql.DOUBLE)
   net_profits_yoy = Column('net_profits_yoy',mysql.DOUBLE)
   kf_net_profits = Column('kf_net_profits',mysql.DOUBLE) #kou fei net_profits
   business_income = Column('business_income',mysql.DOUBLE) #ying ye zong shou ru
   business_income_yoy = Column('business_income_yoy',mysql.DOUBLE) #ying ye zong shou ru tong bi   
   bvps = Column('bvps',mysql.DOUBLE) # mei gu jing zi chan
   roe = Column('roe',mysql.DOUBLE) #jing zi chan shou yi lv
   roe_tb = Column('roe_tb',mysql.DOUBLE) #jing zi chan shou yi lv tan bao
   zc_fz_ratio = Column('zc_fz_ratio',mysql.DOUBLE) #zi chan fu zhai bi lv
   reserved_per_share = Column('reserved_per_share',mysql.DOUBLE) #mei gu zi ben gong ji jin
   reserved_profits_per_share = Column('reserved_profits_per_share',mysql.DOUBLE) #mei gu wei fen pei li run
   epcf = Column('epcf',mysql.DOUBLE) #mei gu jing ying xian jin liu
   net_profit_ratio = Column('net_profit_ratio',mysql.DOUBLE) #jing li lv
   gross_profit_rate = Column('gross_profit_rate',mysql.DOUBLE) #xiao shou mao li lv
   inventory_turnover = Column('inventory_turnover',mysql.DOUBLE) #mao li lv
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.eps = kargs['eps']
        self.net_profits = kargs['net_profits']        
        self.net_profits_yoy = kargs['net_profits_yoy']        
        self.kf_net_profits = kargs['kf_net_profits']
        self.business_income = kargs['business_income']
        self.business_income_yoy = kargs['business_income_yoy']
        self.bvps = kargs['bvps']
        self.roe = kargs['roe']
        self.roe_tb = kargs['roe_tb']
        self.zc_fz_ratio = kargs['zc_fz_ratio']
        self.reserved_per_share = kargs['reserved_per_share']
        self.reserved_profits_per_share = kargs['reserved_profits_per_share']
        self.epcf = kargs['epcf']
        self.net_profit_ratio = kargs['net_profit_ratio']
        self.gross_profit_rate = kargs['gross_profit_rate']
        self.inventory_turnover = kargs['inventory_turnover']

class GDRS(Base):
   """
   Gu Dong Ren Shu
   """
   __tablename__ = 'stock_gdrs_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   gdrs = Column('gdrs',mysql.BIGINT)
   gdrs_jsqbh = Column('gdrs_jsqbh',mysql.DOUBLE)
   rjltg = Column('rjltg',mysql.BIGINT)
   rjltg_jsqbh = Column('rjltg_jsqbh',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.gdrs = kargs['gdrs']
        self.gdrs_jsqbh = kargs['gdrs_jsqbh']
        self.rjltg = kargs['rjltg']
        self.rjltg_jsqbh = kargs['rjltg_jsqbh']

class SDGD(Base):
   """
   Shi Da Gu Dong
   """
   __tablename__ = 'stock_sdgd_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   sid = Column('sid',mysql.NVARCHAR(6),primary_key=True)
   gdmc = Column('gdmc',mysql.NVARCHAR(200))
   cgbl = Column('cgbl',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.sid = kargs['sid']
        self.gdmc = kargs['gdmc']
        self.cgbl = kargs['cgbl']

class SDLTGD(Base):
   """
   Shi Da Liu Tong Gu Dong
   """
   __tablename__ = 'stock_sdltgd_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   sid = Column('sid',mysql.NVARCHAR(6),primary_key=True)
   gdmc = Column('gdmc',mysql.NVARCHAR(200))
   cgbl = Column('cgbl',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.sid = kargs['sid']
        self.gdmc = kargs['gdmc']
        self.cgbl = kargs['cgbl']

class XSG(Base):
   """
   Xian Shou Gu
   """
   __tablename__ = 'stock_xsg_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   count = Column('count',mysql.DOUBLE)
   ratio = Column('ratio',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.count = kargs['count']
        self.ratio = kargs['ratio']

class Forecast(Base):
   """
   Ye Ji Yu Gao
   """
   __tablename__ = 'stock_forecast_data'
   date = Column('date',mysql.DATE,primary_key=True)
   code = Column('code',mysql.NVARCHAR(6), primary_key=True)
   report_date = Column('report_date',mysql.DATE)
   forecast_type = Column('forecast_type',mysql.TEXT)
   pre_eps = Column('pre_eps',mysql.DOUBLE)
   min_range = Column('min_range',mysql.DOUBLE)
   max_range = Column('max_range',mysql.DOUBLE)
   def __init__(self, kargs):
        """"""
        self.date = kargs['date']
        self.code = kargs['code']
        self.report_date = kargs['report_date']
        self.forecast_type = kargs['forecast_type']
        self.pre_eps = kargs['pre_eps']
        self.min_range = kargs['min_range']
        self.max_range = kargs['max_range']

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
