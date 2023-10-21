# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    size = scrapy.Field()
    outlinks = scrapy.Field()
    content_type = scrapy.Field()