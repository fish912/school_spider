ES_CFG = {
    "ES_ADDRESS": "http://10.240.60.70:9200/",
    "INDEX_NAME": 'yk_cs7',
    "DOC_TYPE": '_doc',
    "USERNAME": None,
    "PASSWORD": None,
}

# redis服务器配置
REDIS_CFG = {
    "REDIS_HOST": "10.240.60.70:6379",
    "PASSWORD": "antiy",
    "DB": 0
}

REDIS_RULE_KEY = "_spider_rule"

# redis服务器配置
MONGO_CFG = {
    "HOST": "10.240.60.70",
    "PORT": 27017,
    "USERNAME": "admin",
    "PASSWORD": "123456",
    "DB": "spider"
}

ALLOW_DOMAINS = ['xacom.edu.cn']
NEED_PIC = False
