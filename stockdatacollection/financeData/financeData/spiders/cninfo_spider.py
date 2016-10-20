import re
import logging
import scrapy
import tushare as ts
import pandas as pd
import numpy as np
from financeData.items import *
from scrapy.selector import Selector
from scrapy.utils.response import get_base_url

class CNINFOSpider(scrapy.Spider):
   name = 'CNINFO'
   allowed_domains = ['www.cninfo.com.cn']

   def start_requests(self):
      df = ts.get_stock_basics()
      codes = list(df.index)
      for code in ['000002']:
         yield scrapy.Request('http://stockpage.10jqka.com.cn/%s/holder/' % code, self.parse)

   def parse(self, response):
      selector = Selector(response)
      base_url = get_base_url(response)
      m=re.match('.*([0-9]{6}).*',base_url)
      code = m.group(1)
      gdgbItem = THSGDGBItem()
      gdrs_df = self.getGuDongRenShu(selector,code)
      gdgbItem['df'] = gdrs_df
      return [gdgbItem]
      
   def getGuDongRenShu(self,sel,code):
      #Get Gu dong ren shu report date
      divs = sel.xpath('//div[@class="data_tbody"]')
      table1 = divs.xpath('.//table[@class="top_thead"]')
      gdrsDates = np.array(table1[0].xpath('.//tr/th/div[@class="td_w"]/text()').extract())
      codeList = np.array([code for i in range(len(gdrsDates))])
      table2 = divs.xpath('.//table[@class="tbody"]')
      trs = table2[0].xpath('.//tr')
      gdzrs = trs[0].xpath('.//td/div[@class="td_w"]/text()').extract()
      gdrs_jsqbh = []
      tds = trs[1].xpath('.//td')
      for td in tds:
         if len(td.xpath('.//span')) > 0:
            gdrs_jsqbh += td.xpath('.//span/text()').extract()
         else:
            #gdrs_jsqbh += td.xpath('.//text()').extract()
            gdrs_jsqbh += [0]
      rjltg = trs[2].xpath('.//td/text()').extract()
      rjltg_jsqbh = []
      tds = trs[3].xpath('.//td')
      for td in tds:
         if len(td.xpath('.//span')) > 0:
            rjltg_jsqbh += td.xpath('.//span/text()').extract()
         else:
            #rjltg_jsqbh += td.xpath('.//text()').extract()
            rjltg_jsqbh += [0]
      gdzrs = np.array([int(float(i)*10000) if float(i)<500 else int(float(i)) for i in gdzrs])
      print '-----------------'
      print gdzrs
      print gdrs_jsqbh
      gdrs_jsqbh = np.array([float(str(i).strip('+|%')) for i in gdrs_jsqbh])
      rjltg = np.array([int(float(i)*10000) if float(i)<500 else int(float(i)) for i in rjltg])
      rjltg_jsqbh = np.array([float(str(i).strip('+|%')) for i in rjltg_jsqbh])
      arrays = [gdrsDates,codeList]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
      s = pd.Series(gdzrs, index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "gdrs"},inplace=True)
      print '-----------------'
      print gdzrs
      print gdrs_jsqbh
      print gdrsDates
      print codeList
      f['gdrs_jsqbh'] = gdrs_jsqbh
      f['rjltg'] = rjltg
      f['rjltg_jsqbh'] = rjltg_jsqbh
      return f