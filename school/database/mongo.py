import datetime
import hashlib
import pickle
import time

import pymongo
from school.config.config import MONGO_CFG

from dateutil import parser


class MongoHelper(object):

    def __init__(self, host, port, username, password, db):
        self.mongo = pymongo.MongoClient(f'mongodb://{username}:{password}@{host}:{port}')
        self.spider_db = self.mongo['spider']
        self.finger_col = self.spider_db['fingerprint']
        self.rule_col = self.spider_db['rule']
        self.suspicious_col = self.spider_db['suspicious']

    def get_client(self):
        return self.mongo

    def insert_rule(self, allow=None, deny=None, allow_domains=None, deny_domains=None, follow=False,
                    dont_filter=False, update=True):
        unique_id = hashlib.md5(f'{allow}'.encode()).hexdigest()

        res = self.rule_col.update_one({"unique_id": unique_id}, {"$set": {
            "allow": allow or list(),
            "deny": deny or list(),
            "allow_domains": allow_domains or list(),
            "deny_domains": deny_domains or list(),
            "follow": follow,
            "dont_filter": dont_filter,
            "unique_id": unique_id,
            "update": update
        }}, upsert=True)
        return res.raw_result.get('updatedExisting')

    def get_rule_filter_by_id(self, filter_id: list):
        res = self.rule_col.find({"unique_id": {"$nin": filter_id}}, {"_id": 0})
        return res

    def get_rule_unique_id_set(self):
        res = self.rule_col.find({}, {"unique_id": 1, "_id": 0}).distinct("unique_id")
        return set(res)

    def get_rule_filter_by_update(self):
        res = self.rule_col.find({"update": True}, {"_id": 0})
        return res

    def update_rule_state(self, unique_id_list):
        if unique_id_list:
            self.rule_col.update({"unique_id": {"$in": unique_id_list}}, {"$set": {"update": False}})

    def insert_finger(self):
        self.finger_col.insert_one({})

    def exist_finger(self, url_finger, html_finger, last_html, org_url):
        exist = self.finger_col.find_one({"url": url_finger})
        if not exist:
            self.finger_col.insert_one({
                "url": url_finger,
                "html": html_finger,
                "org_url": org_url,
                "update_time": parser.parse(str(datetime.datetime.now())),
                "create_time": parser.parse(str(datetime.datetime.now())),
                "version": 0,
                "diff_past": "",
                "diff_now": "",
                # "past_html": "",
                "last_html": last_html
            })
            return False
        else:
            is_same, past, now = self.find_dif(exist["last_html"] or "", last_html or "")

            if is_same:
                return True
            self.finger_col.update({"url": url_finger}, {"$set": {
                "html": html_finger,
                "update_time": parser.parse(str(datetime.datetime.now())),
                "diff_past": past,
                "diff_now": now,
                # "past_html": exist["last_html"],
                "last_html_text": last_html,
            }, "$inc": {"version": 1}})
            return False

    def insert_suspicious_msg(self, record):
        self.suspicious_col.update_one({"request_url": record["request_url"]}, {"$set": record}, upsert=True)

    @classmethod
    def find_dif(cls, str1, str2):
        if str1 == str2:
            return True, None, None
        len1 = len(str1)
        len2 = len(str2)
        max_length = max(len1, len2)
        # 定义初始位置的索引
        first = 0
        last = max_length
        while first <= last:
            mid = (first + last) // 2
            if str1[first:mid] == str2[first:mid]:
                first = mid + 1
            elif str1[mid:] == str2[mid:]:
                last = mid
            else:
                for i in range(first, min(len(str1), len(str2))):
                    if str1[i] != str2[i]:
                        first = i
                        break
                str1 = str1[first: last]
                str2 = str2[first: last]
                temp = 0
                for j in range(-1, -min(len(str1), len(str2)), -1):
                    if str1[j] != str2[j]:
                        temp = j
                        break
                if temp == 0:
                    return False, str1, str2
                else:
                    return False, str1[:temp], str2[:temp]
        return True, None, None


MONGO = MongoHelper(MONGO_CFG['HOST'], MONGO_CFG['PORT'], MONGO_CFG['USERNAME'], MONGO_CFG['PASSWORD'], MONGO_CFG['DB'])

if __name__ == '__main__':
    # print(MONGO.insert_rule(allow_domains=['xjtu.edu.cn'], follow=False))
    print(MONGO.insert_rule(allow=['https://www.baidu.com/'], follow=False, dont_filter=False))
    # print(MONGO.insert_rule(allow_domains=['xjtu.edu.cn'], follow=True))
    # print(MONGO.insert_rule(allow_domains=['xjtu.edu.cn'], follow=True, dont_filter=False))
    print(MONGO.get_rule_unique_id_set())
