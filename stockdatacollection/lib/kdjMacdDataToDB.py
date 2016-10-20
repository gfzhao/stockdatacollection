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
      
   def kdjMacdToDatabase(self,code,xlFile,table): 
      #wb = open_workbook('/home/gfzhao/tmp/tdx_export_data/kdjOverSell/week/week_000001.xls')
      print code,xlFile,table
      try:
         fd = open(xlFile,'r')
         validLines = fd.readlines()[2:-1]
         nRows = len(validLines)
      except:
         print "Error to open file %s" % xlFile
      maxDateInDB = None
      maxDates = session.execute("select date from %s where code = %s order by date desc limit 1" % (table,code)).first()
      print maxDates,'++++++++++++'
      if maxDates != None and len(maxDates) != 0:
         maxDateInDB = maxDates[0]
      reportMap = {}
      keyColMap = {0:'date',1:'open',2:'high',3:'low',4:'close',-7:'dif',-6:'dea',-5:'macd',-4:'k',-3:'d',-2:'j',-1:'j_chaomai_flag'}
      for key in keyColMap.values():
         reportMap[key] = []
      reportMap['code'] = []
      rowLineNums = range(nRows)
      rowLineNums.reverse()
      for row in rowLineNums[:-2]:
         validLine = validLines[row].split()
         nCols = len(validLine)
         date = validLine[0]
         if maxDateInDB != None:
            if dateutil.parser.parse(str(maxDateInDB))>=dateutil.parser.parse(date):
               break
         tmpKeys = []
         for key in keyColMap.keys():
            if key < 0:
               key = nCols + key
            tmpKeys.append(key)
         for col in range(nCols):
            if col not in tmpKeys:
               continue
            cellValue = validLine[col]
            if cellValue == date:
               cellValue.replace('/','-')
            if col > 4:
               reportMap[keyColMap[col-nCols]].append(cellValue)
            else:
               reportMap[keyColMap[col]].append(cellValue)
         reportMap['code'].append(code)
      for key in reportMap.keys():
         print key,len(reportMap[key])
      if len(reportMap['code']) != 0:
         reportNumpyArrMap = {}
         for key in keyColMap.values()+['code']:
            reportNumpyArrMap[key] = np.array(reportMap[key])
         arrays = [reportNumpyArrMap['date'],reportNumpyArrMap['code']]
         tuples = list(zip(*arrays))
         index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
         s = pd.Series(reportNumpyArrMap['open'], index=index)
         f = s.to_frame()
         f.rename(index=str, columns={0: "open"},inplace=True)
         for key in keyColMap.values()+['code']:
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
      for xlFile in xlFiles:
         m = re.match('.*_(.*)\.xls',xlFile)
         if m:
            code = m.group(1)
         print code,"-------code---------"
         xlFileFullPath = os.path.join(filePath,xlFile)
         print xlFileFullPath,'======xlFileFullPath============'
         if os.path.isfile(xlFileFullPath):
            if fetdb.kdjMacdToDatabase(code,xlFileFullPath,table) == True:
               validStockNum = validStockNum + 1
   print '----validStockNum: ',validStockNum