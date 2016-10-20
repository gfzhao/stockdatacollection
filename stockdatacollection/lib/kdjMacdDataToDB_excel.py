import pandas as pd
import numpy as np
from xlrd import open_workbook
import datetime
import dateutil.parser

from lib.mysqlHandle import *
from lib.util import *

mh = MysqlHandle()
session = mh.session
database_engine =  MysqlHandle.engine

class KdjMacdExcelToDatabase:
   def __init__(self):
      pass
      
   def kdjMacdToDatabase(self,code,xlFile,table,xlSheet): 
      #wb = open_workbook('/home/gfzhao/tmp/tdx_export_data/kdjOverSell/week/week_000001.xls')
      print code,xlFile,table,xlSheet
      wb = open_workbook(xlFile)
      sheet=wb.sheet_by_name(xlSheet)
      nRows = sheet.nrows
      nCols = sheet.ncols
      maxDateInDB = None
      maxDates = session.execute("select date from %s where code = %s order by date desc limit 1" % (table,code)).first()
      print maxDates,'++++++++++++'
      if len(maxDates) != 0:
         maxDateInDB = maxDates[0]
      reportMap = {}
      keyColMap = {0:'date',1:'open',2:'high',3:'low',4:'close',16:'dif',17:'dea',18:'macd',19:'k',20:'d',21:'j',22:'j_chaomai_flag'}
      for key in keyColMap.values():
         reportMap[key] = []
      reportMap['code'] = []
      for row in range(nRows)[4:-1].reverse():
         date = cellValue = sheet.cell(row,0).value
         if maxDateInDB != None:
            if dateutil.parser.parse(maxDateInDB) >= dateutil.parser.parse(date):
               break
         for col in range(nCols):
            if col not in keyColMap.keys():
               continue
            cellValue = sheet.cell(row,col).value
            if cellValue == date:
               cellValue.replace('/','-')
            reportMap[keyColMap[col]].append(cellValue)
         reportMap['code'].append(code)

      reportNumpyArrMap = {}
      for key in keyColMap.values():
         reportNumpyArrMap[key] = np.array(reportMap[key])
      arrays = [reportNumpyArrMap['date'],reportNumpyArrMap['code']]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
      s = pd.Series(reportNumpyArrMap['open'], index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "open"},inplace=True)
      for key in keyList:
         """
         print '-------key: ',key
         print len(reportNumpyArrMap['date'])
         print len(reportNumpyArrMap[key])
         """
         if key not in ['date','code','open']:
            f[key] = reportNumpyArrMap[key]
      f.to_sql(table,database_engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
      return True

if __name__ == "__main__":
   import os,re
   from os import walk
   k_types = ['day','week']
   fetdb = KdjMacdExcelToDatabase()
   validStockNum = 0
   for k_type in k_types:
      table = '%s_kdj_macd' % k_type
      filePath = '/home/gfzhao/tmp/tdx_export_data/kdjOverSell/%s' % k_type
      xlFiles = []
      for (dirpath, dirnames, filenames) in walk(filePath):
          xlFiles.extend(filenames)
          break
      for xlFile in [xlFiles[0]]:
         m = re.match('.*_(.*)\.xls',xlFile)
         if m:
            code = m.group(1)
         print code,"-------code---------"
         xlFileFullPath = os.path.join(filePath,xlFile)
         print xlFileFullPath,'======xlFileFullPath============'
         xlSheet = '%s_%s' % (k_type,code)
         print xlSheet,'******xlSheet******'
         if os.path.isfile(xlFileFullPath):
            print 1111111111111111111111
            if fetdb.kdjMacdToDatabase(code,xlFileFullPath,table,xlSheet) == True:
               validStockNum = validStockNum + 1
   print '----validStockNum: ',validStockNum