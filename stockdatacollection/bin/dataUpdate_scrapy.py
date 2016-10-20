import sys,os
import logging
import scrapy
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


configure_logging(install_root_handler=False)
logging.basicConfig(
    filename='scrapylog.txt',
    format='%(levelname)s: %(message)s',
    level=logging.WARNING
)

currfile = os.path.abspath(sys.argv[0])
currpath = os.path.dirname(currfile)
sys.path.append(currpath)
spidersPath = currpath + '/' + 'financeData'
sys.path.append(spidersPath)

from financeData.spiders.gdgb_spider import *
import stockHelper
from mysqlOrmMapper import *

g_startDate = '2016-06-01'
g_endDate = '2016-09-26'

sh = stockHelper.StockHelper()
engine = stockHelper.StockHelper.engine

if __name__ == "__main__":
   #sh.get_stock_basics()
   """
   codes = getStockCodes(sh.session)
   for code in codes:
      sh.get_hist_daily_trade_data(code,g_startDate,g_endDate)
   #sh.get_stock_report(2014,4)
   #sh.get_stock_report(2015,4)
   #sh.get_stock_report(2016,2)

   """
   #Run scrapy spiders
   # 'gdgb_spider.THSGDGBSpider' is the name of one of the spiders of the project.
   process = CrawlerProcess(get_project_settings())
   process.crawl(THSGDGBSpider)
   process.start() 
   # the script will block here until the crawling is finished
 
