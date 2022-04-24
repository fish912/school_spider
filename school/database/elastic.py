from elasticsearch import Elasticsearch
from elasticsearch import helpers
import logging as logger
from school.config.config import ES_CFG


class EsHelper(object):
    def __init__(self, es_address="http://10.239.50.211:9200/", index_name='yk_cs', doc_type='_doc', username=None,
                 password=None):
        if username:
            self.es = Elasticsearch(hosts=es_address, http_auth=(username, password))
        else:
            self.es = Elasticsearch(hosts=es_address)
        self.index_name = index_name
        self.doc_type = doc_type
        # 统一检查模板
        self.create_template(index_name)
        if not self.check_index_exist(self.index_name):
            self.create_index(self.index_name)

    def create_template(self, index_name, doc_name='_doc'):
        if not self.es.indices.exists_template(index_name):
            try:
                self.es.indices.put_template(index_name, {
                    "settings": {
                        "index": {
                            "refresh_interval": "10s",
                            "number_of_shards": "5",
                            "search.slowlog.threshold.query.warn": "10s",
                            "search.slowlog.threshold.query.info": "5s",
                            "search.slowlog.threshold.fetch.warn": "3s",
                            "search.slowlog.threshold.fetch.info": "1s"
                        }
                    },
                    # "index_patterns": [f"{index_name}*"],
                    "mappings": {
                        index_name: {
                            "properties": {
                                "title": {
                                    "type": "text",
                                    "analyzer": "ik_max_word",
                                    "search_analyzer": "ik_max_word",
                                },
                                "content": {
                                    "type": "text",
                                    "analyzer": "ik_max_word",
                                    "search_analyzer": "ik_max_word",
                                },
                                "url": {
                                    "type": "keyword"
                                },
                            }
                        }
                    }
                })
            except Exception as e:
                logger.error(e, exc_info=True)
                raise e

    def bulk_action(self, actions: list):
        try:
            res = helpers.bulk(self.es, actions, max_retries=3, stats_only=True)
            if len(actions) != res[0]:
                logger.error(f'failed bulk into es: {res}')
                raise Exception('failed bulk into es')
        except Exception as e:
            logger.error(e, exc_info=True)

    def check_index_exist(self, index_name) -> bool:
        return self.es.indices.exists(index_name)

    def get_one_es_action(self, id_, data: dict):
        action = {
            "_op_type": "index",
            "_index": self.index_name,
            "_type": "_doc",
            "_id": id_,
            "_source": data
        }
        return action

    def create_index(self, index_name, index_alias=None):
        self.es.indices.create(index=index_name)
        if index_alias is not None:
            self.es.indices.put_alias(index=index_name, name=index_alias)


ES = EsHelper(ES_CFG['ES_ADDRESS'], ES_CFG['INDEX_NAME'], ES_CFG['DOC_TYPE'], ES_CFG['USERNAME'], ES_CFG['PASSWORD'])
