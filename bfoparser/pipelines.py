# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy
from pymongo import MongoClient
from pymongo import errors

class BfoparserPipeline:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        # бд создаётся автоматически
        self.mongobase = client.bo_nalog

    def process_item(self, item, spider):
        collection = self.mongobase[spider.name]
        try:
            # важно - разобраться со вложенностью! - в item одно поле = словарь
            collection.insert_one(item['org_info'])
        except errors.DuplicateKeyError:
            pass
        return item

