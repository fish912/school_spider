# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SchoolItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    host = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    id = scrapy.Field()
    url_fingerprint = scrapy.Field()
    html_fingerprint = scrapy.Field()
