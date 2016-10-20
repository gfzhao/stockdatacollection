# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item,Field

class THSGDGBItem(Item):
   # define the fields for your item here like:
   # name = Fscrapy.Field()
   code = Field()
   dfs = Field()

class FinanceExcelFileItem(Item):
    # ... other item fields ...
    file_urls = Field()
    file_paths = Field()