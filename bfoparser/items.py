# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

# обрабатывать, кажется, ничего не нужно...
# немного не понимаю, у меня org_data - это весь целый словарь
class BfoparserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    org_info = scrapy.Field()
