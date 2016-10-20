import sys
import datetime
import dateutil.parser

from model.mysqlOrmMapper import *
from model.kdjMacdOrmMapper import *

def str2Class(str):
   return getattr(sys.modules[__name__], str)

def getStockTimeToMarket(session,code):
   res = session.query(str2Class('StockBasic')).filter_by(code=code).all()
   if len(res) == 0:
      return None
   return str(res[0].timeToMarket)

def getStockTimeToMarket(session,code):
   res = session.query(str2Class('StockBasic')).filter_by(code=code).all()
   if len(res) == 0:
      return None
   return str(res[0].timeToMarket)

def DFToSQLOneByOne(session,df,ormClasss):
   """
   Store stock data from pandas dataframe format to database one by one
   to avoid duplicate entry in the database
   """
   retItems = session.query(str2Class(ormClasss)).filter_by(code=code).all()
   tableName = str2Class(ormClasss).__tablename__
   if len(retItems) == 0:
      if len(df.index.names) == 3:
         df.to_sql(tableName,database_engine,if_exists='append',
                index=True,index_label=['date','code','sid'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE,'sid':mysql.NVARCHAR(6)})
      else:
         df.to_sql(tableName,database_engine,if_exists='append',
                index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
   else:
      dateList = []
      for i in range(len(df)):
         df_date = df.index.values[i][0]
         if df_date not in dateList:
            dateList.append(df_date)
      tobeDeleteDateList = []
      for date in dateList:
         retItems_2 = session.query(str2Class(ormClasss)).filter_by(code=code).filter_by(date=date).all()
         if len(retItems) != 0:
            tobeDeleteDateList.append(date)
      new_df = df.drop(tobeDeleteDateList)
      if len(df.index.names) == 3:
         new_df.to_sql(tableName,database_engine,if_exists='append',
                index=True,index_label=['date','code','sid'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE,'sid':mysql.NVARCHAR(6)})
      else:
         new_df.to_sql(tableName,database_engine,if_exists='append',
                index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})

def DFToSQLSkipExisted(session,df,ormClasss):
   """
   To be enhanced
   """
   for i in range(len(df)):
      df_date,df_code = df.index.values[i]
      retItems = session.query(str2Class(ormClasss)).filter_by(date=df_date).filter_by(code=df_code).all()
      if len(retItems) != 0:
         df.drop(df.index[[i]],inplace=True)
   f.to_sql(str2Class(ormClasss).__tablename__,database_engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})

def getAllStockCodes(session):
   res = session.query(str2Class('StockBasic')).all()
   codes = []
   for item in res:
      codes.append(item.code)
   return codes

# if date1 > date2, return True, if date1 =< date2 return False
def dateCompare(date1,date2):
    dt1 = dateutil.parser.parse(date1)
    dt2 = dateutil.parser.parse(date2)
    ret = True
    if int(dt1.year) > int(dt2.year):
      ret = True
    elif int(dt1.year) < int(dt2.year):
      ret = False
    else:
      if int(dt1.month) > int(dt2.month):
         ret = True
      elif int(dt1.month) < int(dt2.month):
         ret = False
      else:
         if int(dt1.day) > int(dt2.day):
            ret = True
         else:
            ret = False
    return ret

# this function has not been ready for use, it's still under debug
def get_all(cls, session, columns=None, offset=None, limit=None, order_by=None, lock_mode=None):
   if columns:
      if isinstance(columns, (tuple, list)):
          query = session.query(*columns)
      else:
          query = session.query(columns)
          if isinstance(columns, str):
              query = query.select_from(cls)
   else:
      query = session.query(cls)
   if order_by is not None:
      if isinstance(order_by, (tuple, list)):
          query = query.order_by(*order_by)
      else:
          query = query.order_by(order_by)
   if offset:
      query = query.offset(offset)
   if limit:
      query = query.limit(limit)
   if lock_mode:
      query = query.with_lockmode(lock_mode)
   return query.all()
 
if __name__ == "__main__":
   from lib.mysqlHandle import *

   mh = MysqlHandle()
   session = mh.session
   database_engine =  MysqlHandle.engine
   code = '603600'
   sr = session.execute("select date from stock_finance_data where code = %s order by date desc limit 1" % code)
   print session.execute("select timeToMarket from stock_basics order by timetomarket desc limit 1").first()[0]
   print sr.first()
   #print get_all(StockBasic,session,'timeToMarket',None,1,'timeToMarket desc',None)
   #query = session.query(StockBasic)
   #print query
   #print query.statement
   #for item in query:
   #   print item.name
   #print query.all()
   #print query.first().name