"""
redis连接工具
"""
import json
import random
import time

from redis import Redis, StrictRedis
from rediscluster import RedisCluster
from school.config.config import REDIS_CFG


class RedisUtil(object):
    __instance = None

    def __new__(cls, cluster_model=False):
        if not cls.__instance:
            cls.__instance = super(RedisUtil, cls).__new__(cls)
        return cls.__instance

    def __init__(self, cluster_model=False) -> None:
        password = REDIS_CFG.get("PASSWORD")
        if cluster_model:
            hosts = REDIS_CFG.get("CLUSTER_HOST")
            startup_nodes = []
            for i in hosts:
                host, port = i.split(":")
                startup_nodes.append({
                    "host": host,
                    "port": port
                })
            if password:
                self.rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, password=password)
            else:
                self.rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        else:
            host = REDIS_CFG.get("REDIS_HOST")
            host, port = host.split(":")
            if password:
                self.rc = StrictRedis(host=host, port=int(port), db=int(REDIS_CFG.get("DB")),
                                      password=password)
            else:
                self.rc = StrictRedis(host=host, port=int(port), db=int(REDIS_CFG.get("DB")))

    def get_redis(self):
        """
        redis连接池
        :param db: 连接的db库
        :return: redis连接句柄
        """
        # 判断切换db并生成新的链接
        return self.rc


REDIS = RedisUtil()
