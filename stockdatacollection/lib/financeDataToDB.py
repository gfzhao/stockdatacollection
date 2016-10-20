import pandas as pd
import numpy as np
from xlrd import open_workbook

from lib.mysqlHandle import *
from lib.util import *

mh = MysqlHandle()
session = mh.session
database_engine =  MysqlHandle.engine

class FinanceExcelToDatabase:
   def __init__(self):
      pass

   def xlToDatabase(self,code,xlFile,table,xlSheet):   
      #wb = open_workbook('/home/gfzhao/stockData/financeData/f_603600.xls')
      wb = open_workbook(xlFile)
      sheet=wb.sheet_by_name(xlSheet)
      nRows = sheet.nrows
      nCols = sheet.ncols
      timeToMarket = getStockTimeToMarket(session,code)
      #if timeToMarket == None:  # TBD
      if int(timeToMarket) == 0:
         return False
      foundCol = None
      for col in range(nCols)[1:]:
         tmpDate = str(sheet.cell(0,col).value)
         if dateCompare(timeToMarket,tmpDate) == True:
            break
         foundCol = col+1 #python is from 0, but excel is from 1
      if foundCol == None:
         return False
      reportMap = {}
      keyList = ['date','eps','net_profits','net_profits_yoy','kf_net_profits','business_income',
         'business_income_yoy','bvps','roe','roe_tb','zc_fz_ratio','reserved_per_share',
         'reserved_profits_per_share','epcf','gross_profit_rate','inventory_turnover','code','net_profit_ratio']
      rowKeyMap = {}
      for idx in range(len(keyList))[:-2]:
         key = keyList[idx]
         rowKeyMap[idx] = key
         reportMap[key] = []
      reportMap['code'] = []
      reportMap['net_profit_ratio'] = []
      for col in range(foundCol)[1:]:
         for row in range(nRows):
            key = rowKeyMap[row]
            cellValue = sheet.cell(row,col).value
            if cellValue == '':
               cellValue = 0
            #print 'code: ',code,'row: ',row+1,', col: ',col+1
            if row == 0:
               reportMap[key].append(str(cellValue).replace('-',''))
            else:
               reportMap[key].append(float(cellValue))
         reportMap['code'].append(code)
         if reportMap['business_income'][-1] != 0:
            net_profit_ratio = reportMap['net_profits'][-1]/reportMap['business_income'][-1]
         else:
            net_profit_ratio = 0.0
         reportMap['net_profit_ratio'].append(net_profit_ratio)
      #if no data in the excel file, then set them to 0
      for key in keyList[:-2]:
         if len(reportMap[key]) == 0:
            reportMap[key] = [0 for idx in range(len(reportMap['date']))]

      reportNumpyArrMap = {}
      for key in keyList:
         reportNumpyArrMap[key] = np.array(reportMap[key])
      arrays = [reportNumpyArrMap['date'],reportNumpyArrMap['code']]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
      s = pd.Series(reportNumpyArrMap['eps'], index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "eps"},inplace=True)
      for key in keyList:
         """
         print '-------key: ',key
         print len(reportNumpyArrMap['date'])
         print len(reportNumpyArrMap[key])
         """
         if key not in ['date','code','eps']:
            f[key] = reportNumpyArrMap[key]
      f.to_sql(table,database_engine,if_exists='append',
                  index=True,index_label=['date','code'],dtype={'code':mysql.NVARCHAR(6),'date':mysql.DATE})
      return True

if __name__ == "__main__":
   table = 'stock_finance_data'
   xlSheet = 'new-finance-data'
   fetdb = FinanceExcelToDatabase()
   codes = getAllStockCodes(session)
   validStockNum = 0
   for code in codes:
      print code,"----------------"
      xlFile = '/home/gfzhao/stockData/financeData/f_%s.xls' % code
      if os.path.isfile(xlFile):
         if fetdb.xlToDatabase(code,xlFile,table,xlSheet) == True:
            validStockNum = validStockNum + 1
   print '----validStockNum: ',validStockNum