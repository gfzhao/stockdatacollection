#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import logging
import scrapy
import tushare as ts
import pandas as pd
import numpy as np
import sys
from financeData.items import *
from scrapy.selector import Selector
from scrapy.utils.response import get_base_url
from scrapy.utils.log import configure_logging

class THSGDGBSpider(scrapy.Spider):
   name = 'THSGDGB'
   custom_settings = {
        'ITEM_PIPELINES': {
            'financeData.pipelines.GDGBPipeline': 300
        }
    }
   allowed_domains = ['stockpage.10jqka.com.cn']

   def start_requests(self):
      df = ts.get_stock_basics()
      codes = list(df.index)
      for code in codes:
         yield scrapy.Request('http://stockpage.10jqka.com.cn/%s/holder/' % code, self.parse)

   def parse(self, response):
      selector = Selector(response)
      base_url = get_base_url(response)
      m=re.match('.*([0-9]{6}).*',base_url)
      code = m.group(1)
      gdgbItem = THSGDGBItem()
      dfs = []
      gdrs_df = self.getGuDongRenShu(selector,code)
      sdgd_df = self.getShiDaGuDong(selector,code)
      sdltgd_df = self.getShiDaLiuTongGuDong(selector,code)
      dfs.append({'df':gdrs_df,'table':'stock_gdrs_data'})
      dfs.append({'df':sdgd_df,'table':'stock_sdgd_data'})
      dfs.append({'df':sdltgd_df,'table':'stock_sdltgd_data'})
      gdgbItem['dfs'] = dfs
      gdgbItem['code'] = code
      """
      print "-------------------------------------"
      sdgd_df = self.getShiDaLiuTongGuDong(selector,code)
      gdgbItem['df'] = sdgd_df
      print "********************************************"
      """
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
      gdrs_jsqbh = np.array([float(str(i).strip('+|%')) for i in gdrs_jsqbh])
      rjltg = np.array([int(float(i)*10000) if float(i)<500 else int(float(i)) for i in rjltg])
      rjltg_jsqbh = np.array([float(str(i).strip('+|%')) for i in rjltg_jsqbh])
      arrays = [gdrsDates,codeList]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code'])
      s = pd.Series(gdzrs, index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "gdrs"},inplace=True)
      f['gdrs_jsqbh'] = gdrs_jsqbh
      f['rjltg'] = rjltg
      f['rjltg_jsqbh'] = rjltg_jsqbh
      return f

   def getShiDaGuDong(self,sel,code):
      #Get Gu dong ren shu report date
      divs = sel.xpath('//div[@id="bd_0"]')
      divs_2 = divs.xpath('.//div[@class="m_tab mt15"]')
      dateList = divs_2.xpath('.//a/text()').extract()
      ids = []
      for i in range(len(dateList)):
         number = i+1
         ids.append('ther_%s' % number)
      guDongMingChengs = []
      chiGuBiLis = []
      dates = []
      codes = []
      sids = []
      index = 0
      for id in ids:
         divs_3 = divs.xpath('.//div[@id="%s"]' % id)
         tables=divs_3.xpath('.//table[@class="m_table m_hl ggintro"]')
         tbodys=tables.xpath('.//tbody')
         trs=tbodys.xpath('.//tr')
         date = dateList[index]
         index += 1
         sid = 0
         for tr in trs:
            gdmc = tr.xpath('.//th/a/text()').extract()[0]
            gdmc = gdmc.replace("'","")
            tds = tr.xpath('.//td/text()').extract()
            cgbl = tds[2]
            guDongMingChengs.append(gdmc)
            chiGuBiLis.append(float(cgbl))
            dates.append(date)
            codes.append(code)
            sids.append(str(sid))
            sid += 1
      dates = np.array(dates)
      codes = np.array(codes)
      guDongMingChengs = np.array(guDongMingChengs)
      chiGuBiLis = np.array(chiGuBiLis)
      arrays = [dates,codes,sids]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code', 'sid'])
      s = pd.Series(guDongMingChengs, index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "gdmc"},inplace=True)
      f['cgbl'] = chiGuBiLis
      return f
      
   def getShiDaLiuTongGuDong(self,sel,code):
      #Get Gu dong ren shu report date
      divs = sel.xpath('//div[@id="bd_1"]')
      divs_2 = divs.xpath('.//div[@class="m_tab mt15"]')
      dateList = divs_2.xpath('.//a/text()').extract()
      ids = []
      for i in range(len(dateList)):
         number = i+1
         ids.append('fher_%s' % number)
      guDongMingChengs = []
      chiGuBiLis = []
      dates = []
      codes = []
      sids = []
      index = 0
      for id in ids:
         divs_3 = divs.xpath('.//div[@id="%s"]' % id)
         tables=divs_3.xpath('.//table[@class="m_table m_hl ggintro"]')
         tbodys=tables.xpath('.//tbody')
         trs=tbodys.xpath('.//tr')
         date = dateList[index]
         index += 1
         sid = 0
         for tr in trs:
            gdmc = tr.xpath('.//th/a/text()').extract()[0]
            gdmc = gdmc.replace("'","")
            tds = tr.xpath('.//td/text()').extract()
            cgbl = tds[2]
            guDongMingChengs.append(gdmc)
            chiGuBiLis.append(float(cgbl))
            dates.append(date)
            codes.append(code)
            sids.append(str(sid))
            sid += 1
      dates = np.array(dates)
      codes = np.array(codes)
      guDongMingChengs = np.array(guDongMingChengs)
      chiGuBiLis = np.array(chiGuBiLis)
      arrays = [dates,codes,sids]
      tuples = list(zip(*arrays))
      index = pd.MultiIndex.from_tuples(tuples, names=['date', 'code', 'sid'])
      s = pd.Series(guDongMingChengs, index=index)
      f = s.to_frame()
      f.rename(index=str, columns={0: "gdmc"},inplace=True)
      f['cgbl'] = chiGuBiLis
      return f
