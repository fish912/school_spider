import datetime
import logging
from multiprocessing import Process

from scrapy import signals
from school.database.redis_db import REDIS

logger = logging.getLogger(__name__)


class SpiderOpenCloseLogging:

    def __init__(self, crawler):
        self.crawler = crawler
        self.items_scraped = 0
        self.now = datetime.datetime.now()
        self.task_time_now = datetime.datetime.now()

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        # crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        # crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        # crawler.signals.connect(ext.engine_stopped, signal=signals.engine_stopped)
        # crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)

        return ext

    def spider_idle(self, spider):
        # 爬虫队列闲置时
        now = datetime.datetime.now()
        if now - self.now > datetime.timedelta(seconds=30):
            REDIS.get_redis().delete("school_spider:dupefilter")
            self.now = now
        if now - self.task_time_now > datetime.timedelta(hours=2):
            if REDIS.get_redis().llen('school_spider:start_urls') == 0:
                REDIS.get_redis().lpush('school_spider:start_urls', "https://www.xacom.edu.cn/")
                self.task_time_now = now

    def engine_stopped(self):
        # 爬虫关闭
        pass

    def spider_opened(self, spider):
        if REDIS.get_redis().llen('school_spider:start_urls') == 0:
            REDIS.get_redis().lpush('school_spider:start_urls', "https://www.xacom.edu.cn/")
        logger.info("opened spider %s", spider.name)

    def spider_closed(self, spider):
        logger.info("closed spider %s", spider.name)

    def item_scraped(self, item, spider):
        stats = self.crawler.stats.get_stats()
        d = {
            'log_info': stats.get('log_count/INFO', 0),
            'dequeued': stats.get('scheduler/dequeued/redis', 0),
            'log_warning': stats.get('log_count/WARNING', 0),
            'requested': stats.get('downloader/request_count', 0),
            'request_bytes': stats.get('downloader/request_bytes', 0),
            'response': stats.get('downloader/response_count', 0),
            'response_bytes': stats.get('downloader/response_bytes', 0),
            'response_200': stats.get('downloader/response_status_count/200', 0),
            'response_301': stats.get('downloader/response_status_count/301', 0),
            'response_404': stats.get('downloader/response_status_count/404', 0),
            'responsed': stats.get('response_received_count', 0),
            'item': stats.get('item_scraped_count', 0),
            'depth': stats.get('request_depth_max', 0),
            'filtered': stats.get('bloomfilter/filtered', 0),
            'enqueued': stats.get('scheduler/enqueued/redis', 0),
            'spider_name': self.crawler.spider.name
        }
        self.items_scraped += 1
