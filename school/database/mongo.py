import datetime
import hashlib

import pymongo
from school.config.config import MONGO_CFG, ALLOW_DOMAINS
from school.config.keyword import INIT_KEYWORD
from dateutil import parser


class MongoHelper(object):

    def __init__(self, host, port, username, password, db):
        self.mongo = pymongo.MongoClient(f'mongodb://{username}:{password}@{host}:{port}')
        self.spider_db = self.mongo['spider']
        self.finger_col = self.spider_db['fingerprint2']
        self.rule_col = self.spider_db['rule2']
        self.suspicious_col = self.spider_db['suspicious2']
        self.keyword_col = self.spider_db['keyword2']
        self.user_rule_rel_col = self.spider_db['user_rule_rel2']

    def get_client(self):
        return self.mongo

    def get_keyword(self, id_lis: list):
        user_lis = self.user_rule_rel_col.find({"rule": {"$in": id_lis}}).distinct("user")
        if not user_lis:
            return {}
        res = self.keyword_col.find({"user": {"$in": user_lis}})
        res_dic = {}
        for i in res:
            user_dic = res_dic.setdefault(i.get("user"), {0: [], 1: [], 2: []})
            loc = i.get("location")
            word = i.get("word")
            if loc[0] == "1":
                user_dic[0].append(word)
            if loc[1] == "1":
                user_dic[1].append(word)
            if loc[2] == "1":
                user_dic[2].append(word)
        return res_dic

    def insert_rule(self, allow=None, deny=None, allow_domains=None, deny_domains=None, follow=False,
                    dont_filter=False, update=True):
        unique_id = hashlib.md5(f'{allow}{allow_domains}'.encode()).hexdigest()

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
            self.rule_col.update_many({"unique_id": {"$in": unique_id_list}}, {"$set": {"update": False}})

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
            self.finger_col.update_one({"url": url_finger}, {"$set": {
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

    def init_admin_keyword(self):
        update_lis = []
        for k in INIT_KEYWORD:
            update_lis.append(pymongo.UpdateOne({"user": "admin", "word": k}, {"$set":
                                                                                   {"user": "admin", "word": k,
                                                                                    "location": "111",
                                                                                    "time": str(
                                                                                        datetime.datetime.now())[:19],
                                                                                    "is_update": 1}}, upsert=True))
        self.keyword_col.bulk_write(update_lis)


MONGO = MongoHelper(MONGO_CFG['HOST'], MONGO_CFG['PORT'], MONGO_CFG['USERNAME'], MONGO_CFG['PASSWORD'], MONGO_CFG['DB'])
MONGO.insert_rule(allow_domains=ALLOW_DOMAINS, follow=True)
MONGO.init_admin_keyword()

if __name__ == '__main__':
    # print(MONGO.insert_rule(allow_domains=['xjtu.edu.cn'], follow=False))
    # print(MONGO.insert_rule(allow=['https://www.baidu.com/'], follow=False, dont_filter=False))
    print(MONGO.insert_rule(allow_domains=['xacom.edu.cn'], follow=True))
    # print(MONGO.insert_rule(allow_domains=['cnpythons.com'], follow=True, listen_word=["主席"]))
    # print(MONGO.insert_rule(allow_domains=['xjtu.edu.cn'], follow=True, dont_filter=False))
    # print(MONGO.get_rule_unique_id_set())
