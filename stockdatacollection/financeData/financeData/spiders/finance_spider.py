#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import logging
import scrapy
import tushare as ts
import pandas as pd
import numpy as np
import sys
import time
from financeData.items import *
from scrapy.selector import Selector
from scrapy.utils.response import get_base_url
from scrapy.utils.log import configure_logging
from lib.util import *

mh = MysqlHandle()
session = mh.session

class FinanceSpider(scrapy.Spider):
   name = 'Finance'
   download_delay = 0.2
   custom_settings = {
        'ITEM_PIPELINES': {
            'financeData.pipelines.MyFilesPipeline': 300
            #'financeData.pipelines.xlFilesPipeline': 500
        }
    }
   allowed_domains = ['stockpage.10jqka.com.cn']

   def start_requests(self):
      yield scrapy.Request('http://stockpage.10jqka.com.cn/', self.parse)

   def parse(self, response):
      codes = getAllStockCodes(session)
      item = FinanceExcelFileItem()
      item['file_urls'] = []
      for code in codes:
         file_url = 'http://basic.10jqka.com.cn/%s/xls/mainreport.xls' % code
         file_name = 'f_%s.xls' % code
         item['file_urls'].append({'file_url':file_url,'file_name':file_name})
      return item