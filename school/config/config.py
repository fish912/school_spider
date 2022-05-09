ES_CFG = {
    "ES_ADDRESS": "http://10.239.50.211:9200/",
    "INDEX_NAME": 'yk_cs5',
    "DOC_TYPE": '_doc',
    "USERNAME": None,
    "PASSWORD": None,
}

# redis服务器配置
REDIS_CFG = {
    "REDIS_HOST": "10.239.50.211:6378",
    "CLUSTER_HOST": ["10.239.50.211:6379", "10.239.50.211:6380", "10.239.50.211:6381"],
    "PASSWORD": "antiy",
    "DB": 0
}

REDIS_RULE_KEY = "_spider_rule"

# redis服务器配置
MONGO_CFG = {
    "HOST": "localhost",
    "PORT": 27017,
    "USERNAME": "admin",
    "PASSWORD": "123456",
    "DB": "rule"
}
