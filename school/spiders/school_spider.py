import json
import pickle
import time
from abc import ABC

import scrapy
import hashlib
from scrapy_redis.spiders import RedisCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from selectolax.parser import HTMLParser

from school.items import SchoolItem
import re
from school.database.redis_db import REDIS
from school.database.mongo import MONGO
from scrapy.http import Request
from scrapy.mail import MailSender
from threading import Thread
from scrapy.signalmanager import SignalManager

def process_links(url_list):
    return url_list


class SchoolSpider(RedisCrawlSpider, ABC):
    name = 'school_spider'
    red = REDIS.rc
    rules = []
    rule_id = list()
    rule_dont_filter = list()
    key_filter = ['西安', '法医']
    key_pattern = r""
    single = object()
    my_signal = SignalManager()

    def __init__(self, *args, **kwargs):
        super(RedisCrawlSpider, self).__init__(*args, **kwargs)
        self.__init_rule()
        self.my_signal.connect(self.__init_key_pattern, signal=self.single)
        self.my_signal.connect(self.update_rule, signal=self.single)
        Thread(target=self.interval_task).start()

    def interval_task(self):
        while 1:
            self.my_signal.send_catch_log(signal=self.single)
            time.sleep(10)

    def __init_key_pattern(self):
        if not self.key_pattern or self.key_pattern.count('|') != len(self.key_filter) - 1:
            key_p = []
            for i, val in enumerate(self.key_filter):
                key_p.append(rf'{val.strip()}')
            self.key_pattern = '|'.join(key_p)
        return self.key_pattern

    def __init_rule(self):
        for one in MONGO.get_rule_filter_by_id([]):
            unique_id = one.get("unique_id")
            if unique_id not in self.rule_id:
                self.rule_id.append(unique_id)
                self.rule_dont_filter.append(one.get("dont_filter"))
                follow = one.get("follow")
                rule = Rule(LinkExtractor(
                    allow=tuple(one.get("allow", ())),
                    deny=tuple(one.get("deny", ())),
                    allow_domains=tuple(one.get("allow_domains", ())),
                    deny_domains=tuple(one.get("deny_domains", ())),
                ), callback='parse_item', follow=follow)
                self._rules.append(rule)
                self._rules[-1]._compile(self)

    def update_rule(self):
        update_id_list = []
        for one in MONGO.get_rule_filter_by_update():
            unique_id = one.get("unique_id")
            dont_filter = one.get("dont_filter")
            follow = one.get("follow")
            allow = tuple(one.get("allow", ()))
            deny = tuple(one.get("deny", ()))
            allow_domains = tuple(one.get("allow_domains", ()))
            deny_domains = tuple(one.get("deny_domains", ()))
            rule = Rule(LinkExtractor(
                allow=allow,
                deny=deny,
                allow_domains=allow_domains,
                deny_domains=deny_domains,
            ), callback='parse_item', follow=follow)
            # 新增rule
            if unique_id not in self.rule_id:
                self.rule_id.append(unique_id)
                self.rule_dont_filter.append(dont_filter)
                self._rules.append(rule)
                self._rules[-1]._compile(self)
            # 更新rule
            else:
                index = self.rule_id.index(unique_id)
                self._rules[index] = rule
                self._rules[index]._compile(self)
                self.rule_dont_filter[index] = dont_filter
                update_id_list.append(unique_id)
        self.__del_rule()
        MONGO.update_rule_state(update_id_list)

    def __del_rule(self):
        res = set(self.rule_id) - MONGO.get_rule_unique_id_set()
        new_rule_id = self.rule_id.copy()
        if res:
            for _id in res:
                index = self.rule_id.index(_id)
                new_rule_id.pop(index)
                self._rules.pop(index)
                self.rule_dont_filter.pop(index)
            self.rule_id = new_rule_id

    def _build_request(self, rule_index, link):
        return Request(
            url=link.url,
            callback=self._callback,
            errback=self._errback,
            meta=dict(rule=rule_index, link_text=link.text),
            dont_filter=self.rule_dont_filter[rule_index],
        )

    def parse_item(self, response):
        org_text = response.text
        if not org_text:
            return
        try:
            tree = HTMLParser(org_text)
        except Exception as e:
            print(e)
            return

        if tree.body is None:
            return None

        for tag in tree.css('script'):
            tag.decompose()
        for tag in tree.css('style'):
            tag.decompose()
        title = ' '.join([tag.text(strip=True) for tag in tree.css('title')])
        text = re.sub(r'\s+', " ", tree.body.text())
        url_fingerprint = hashlib.md5(response.url.encode('utf-8')).hexdigest()
        # html_fingerprint = hashlib.md5(tree.html.encode('utf8')).hexdigest()
        html_fingerprint = hashlib.md5(text.encode('utf8')).hexdigest()

        self.listen(response)
        item = SchoolItem()
        item['url'] = response.url
        item['title'] = title
        item['content'] = text
        item['fingerprint'] = html_fingerprint
        item['id'] = url_fingerprint

        not_update = MONGO.exist_finger(url_fingerprint, html_fingerprint, text, response.url)
        if not_update:
            return
        return item

    def listen(self, response):
        # 监听关键词
        request = response.request
        refer = request.headers.get("Referer", b"").decode()
        request_url = response.url
        meta = response.meta
        link_text = meta.get("link_text")
        depth = meta.get("depth")
        download_slot = meta.get("download_slot")
        if self.key_filter:
            # pa_str = self.__init_key_pattern()
            pa_str = self.key_pattern
            pa = re.compile(pa_str)
            find_in_extra = pa.findall(request_url) or pa.findall(link_text) or pa.findall(refer)
            find_in_text = pa.findall(response.text)

            # mailer = MailSender.from_settings(self.settings)
            # mailer.send(to=["910804316@qq.com"], subject="Some subject", body="Some body", cc=["910804316@qq.com"])
            if find_in_extra or find_in_text:
                record = {
                    "request_url": request_url,
                    "refer": refer,
                    "link_text": link_text,
                    "depth": depth,
                    "download_slot": download_slot,
                    "find_in_extra": '#'.join(list(set(find_in_extra))),
                    "find_in_content": '#'.join(list(set(find_in_text)))
                }
                MONGO.insert_suspicious_msg(record)

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
                for i in range(first, min(str1, str2)):
                    if str1[i] != str2[i]:
                        first = i
                        break
                str1 = str1[first: last]
                str2 = str2[first: last]
                temp = 0
                for j in range(-1, -min(str1, str2), -1):
                    if str1[j] != str2[j]:
                        temp = j
                        break
                if temp == 0:
                    return False, str1, str2
                else:
                    return False, str1[:temp], str2[:temp]
        return True, None, None

    @classmethod
    def make_requests_from_url(cls, url):
        return scrapy.Request(url, dont_filter=True)


if __name__ == '__main__':
    a = hashlib.md5('ad').hexdigest()
    print(a)
