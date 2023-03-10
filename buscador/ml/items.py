# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst,MapCompose
from w3lib.html import remove_tags

class MlItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    price_ml = scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    price_hayamax = scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    link_ml = scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    link_hayamax = scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    unit=scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
    margin=scrapy.Field(input_processor=MapCompose(remove_tags),output_processor=TakeFirst())
