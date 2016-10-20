import sys,os
import logging
from sqlalchemy.orm import mapper
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base

currfile = os.path.abspath(sys.argv[0])
currpath = os.path.dirname(currfile)
sys.path.append(currpath)

from stockHelperLib import StockHelper

sh = StockHelper("stockHelperLib.properties")
database_engine = sh.get_engine() 
Base = declarative_base(database_engine)

class StockBasic(Base):
   __tablename__ = 'stock_basics'
   __table_args__ = {'autoload':True}
   id = Column('id',mysql.BIGINT, primary_key=True)

   def __init__(self, id, code, name, industry, area,
                 pe, outstanding, totals, totalAssets, liquidAssets,
                 fixedAssets, reserved, reservedPerShar, esp, bvps, pb, timeToMarket):
        """"""
        self.id = id
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

def loadSession():
   """"""
   session = sh.get_session()
   return session

if __name__ == "__main__":
   session = loadSession()
   res = session.query(StockBasic).all()
   index = 0
   for item in res:
      index += 1
      print item.code,item.name
   print index