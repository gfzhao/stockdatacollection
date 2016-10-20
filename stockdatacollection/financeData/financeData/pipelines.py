# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os,sys
import scrapy
from sqlalchemy.dialects import mysql
from scrapy.pipelines.files import FilesPipeline
from financeData.settings import *
from lib.financeDataToDB import *
from lib.mysqlHandle import *
database_engine =  MysqlHandle.engine

mh = MysqlHandle()
session = mh.session

class GDGBPipeline(object):
   def process_item(self, item, spider):
      code = item['code']
      dfs = item['dfs']
      print dfs
      #DFToSQLOneByOne(session,df,code,'GDRS')
      for dfItem in dfs:
         df = dfItem['df']
         table = dfItem['table']
         if table == 'stock_gdrs_data':
            df.to_sql(table,database_engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
         else:
            df.to_sql(table,database_engine,if_exists='append',
                  index=True,index_label=['date','code','sid'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE,'sid':mysql.NVARCHAR(6)})

class MyFilesPipeline(FilesPipeline):
   def get_media_requests(self, item, info):
      for file_spec in item['file_urls']:
         yield scrapy.Request(url=file_spec["file_url"], meta={"file_spec": file_spec})

   def file_path(self, request, response=None, info=None):
      return request.meta["file_spec"]["file_name"]

   def item_completed(self, results, item, info):
    file_paths = [x['path'] for ok, x in results if ok]
    if not file_paths:
        raise DropItem("Item contains no files")
    item['file_paths'] = file_paths
    return item

class xlFilesPipeline(object):
   def process_item(self, item, spider):
      table = 'stock_finance_data'
      xlSheet = 'new-finance-data'
      fetdb = FinanceExcelToDatabase()
      validStockNum = 0
      for file_name in item['file_paths']:
         file_fullpath = os.path.join(FILES_STORE,file_name)
         code = file_name[2:8]
         if os.path.exists(file_fullpath):
            if fetdb.xlToDatabase(code,file_fullpath,table,xlSheet) == True:
               validStockNum = validStockNum + 1
      print '----validStockNum: ',validStockNum
      
      
