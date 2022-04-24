# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib

# from twisted.internet.threads import deferToThread
from school.database.elastic import ES


class EsPipeline(object):
    def __init__(self, ):
        self.es = ES

    def process_item(self, item, spider):
        es_id = item['id']
        save_data = item.__dict__['_values']
        self.es.bulk_action([self.es.get_one_es_action(es_id, save_data)])
        return item
