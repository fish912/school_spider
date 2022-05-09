# Scrapy settings for school project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from school.config.mail_setting import *
SPLASH_URL = 'http://127.0.0.1:8050'
BOT_NAME = 'school'

SPIDER_MODULES = ['school.spiders']
NEWSPIDER_MODULE = 'school.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
COOKIES_ENABLED = True
# 是否启用DNS内存缓存
DNSCACHE_ENABLED = True
# 是否收集详细的深度统计信息。如果启用此功能，则在统计信息中收集每个深度的请求数
DEPTH_STATS_VERBOSE = True
# 将对任何单个域执行的并发（即，并发）请求的最大数量，默认8
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# 对于任何站点，将允许爬网的最大深度。如果为零，则不施加限制
# DEPTH_LIMIT = 1
# 蜘蛛抓取完毕后发送Scrapy统计信息的邮箱列表
# STATSMAILER_RCPTS = ['910804316@qq.com']

# Scrapy下载器将执行的并发（即，并发）请求的最大数量，默认16
# CONCURRENT_REQUESTS = 8
# 在项目处理器（也称为“ 项目管道”）中并行处理的最大并发项目数（每个响应），默认100。
# CONCURRENT_ITEMS = 100

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/100.0.4896.75 Safari/537.36',
    "Connection": "keep-alive",
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'school.middlewares.SchoolSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'school.middlewares.SchoolDownloaderMiddleware': 543,
    # 'scrapy_splash.SplashCookiesMiddleware': 723,
    # 'scrapy_splash.SplashMiddleware': 725,
    # 'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}
# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
   'school.scrapy_extension.SpiderOpenCloseLogging': 500,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'school.pipelines.SchoolPipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
# HTTPCACHE_STORAGE = 'scrapy_splash.SplashAwareFSCacheStorage'





# Enables scheduling storing requests queue in redis.
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
# SCHEDULER_FLUSH_ON_START = True
# Ensure all spiders share same duplicates filter through redis.
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'

# Default requests serializer is pickle, but it can be changed to any module
# with loads and dumps functions. Note that pickle is not compatible between
# python versions.
# Caveat: In python 3.x, the serializer must return strings keys and support
# bytes as values. Because of this reason the json or msgpack module will not
# work by default. In python 2.x there is no such issue and you can use
# 'json' or 'msgpack' as serializers.
# SCHEDULER_SERIALIZER = "scrapy_redis.picklecompat"

# Don't cleanup redis queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'

# Alternative queues.
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.FifoQueue'
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.LifoQueue'

# Max idle time to prevent the spider from being closed when distributed crawling.
# This only works if queue class is SpiderQueue or SpiderStack,
# and may also block the same time when your spider start at the first time (because the queue is empty).
# SCHEDULER_IDLE_BEFORE_CLOSE = 10

# Store scraped item in redis for post-processing.
ITEM_PIPELINES = {
    # 'scrapy_redis.pipelines.RedisPipeline': 300,
    'school.pipelines.EsPipeline': 302,
}

# The item pipeline serializes and stores the items in this redis key.
# REDIS_ITEMS_KEY = '%(spider)s:items'

# The items serializer is by default ScrapyJSONEncoder. You can use any
# importable path to a callable object.
import ujson

# REDIS_ITEMS_SERIALIZER = 'ujson.dumps'

# Specify the host and port to use when connecting to Redis (optional).
REDIS_HOST = '10.239.50.211'
REDIS_PORT = 6378
REDIS_PARAMS = {
    'password': 'antiy'
}

# Specify the full Redis URL for connecting (optional).
# If set, this takes precedence over the REDIS_HOST and REDIS_PORT settings.
# REDIS_URL = 'redis://user:pass@hostname:9001'

# Custom redis client parameters (i.e.: socket timeout, etc.)
# REDIS_PARAMS  = {}
# Use custom redis client class.
# REDIS_PARAMS['redis_cls'] = 'myproject.RedisClient'

# If True, it uses redis' ``SPOP`` operation. You have to use the ``SADD``
# command to add URLs to the redis queue. This could be useful if you
# want to avoid duplicates in your start urls list and the order of
# processing does not matter.
# REDIS_START_URLS_AS_SET = True

# Default start urls key for RedisSpider and RedisCrawlSpider.
# REDIS_START_URLS_KEY = '%(name)s:start_urls'

# Use other encoding than utf-8 for redis.
# REDIS_ENCODING = 'latin1'
