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


class SchoolSpider(RedisCrawlSpider, ABC):
    name = 'school_spider'
    red = REDIS.rc
    rules = []
    rule_id = list()
    single = object()
    my_signal = SignalManager()

    def __init__(self, *args, **kwargs):
        super(RedisCrawlSpider, self).__init__(*args, **kwargs)
        self.__init_rule()
        self.my_signal.connect(self.update_rule, signal=self.single)
        Thread(target=self.interval_task).start()

    def interval_task(self):
        while 1:
            self.my_signal.send_catch_log(signal=self.single)
            time.sleep(10)

    @classmethod
    def __init_listen_word(cls, listen_word):
        key_pattern = '|'.join(listen_word)
        key_pattern = r'<.*?(?=' + key_pattern.strip() + r').*?>'
        return key_pattern

    def __init_rule(self):
        for one in MONGO.get_rule_filter_by_id([]):
            unique_id = one.get("unique_id")
            if unique_id not in self.rule_id:
                self.rule_id.append(unique_id)
                follow = one.get("follow")
                listen_word = one.get("listen_word")
                if listen_word:
                    listen_word = self.__init_listen_word(listen_word)
                rule = Rule(LinkExtractor(
                    allow=tuple(one.get("allow", ())),
                    deny=tuple(one.get("deny", ())),
                    allow_domains=tuple(one.get("allow_domains", ())),
                    deny_domains=tuple(one.get("deny_domains", ())),
                ), callback='parse_item', follow=follow,
                    cb_kwargs={"dont_filter": one.get("dont_filter"), "listen_word": listen_word})
                self._rules.append(rule)
                self._rules[-1]._compile(self)

    def update_rule(self):
        update_id_list = []
        for one in MONGO.get_rule_filter_by_update():
            unique_id = one.get("unique_id")
            dont_filter = one.get("dont_filter")
            listen_word = one.get("listen_word")
            if listen_word:
                listen_word = self.__init_listen_word(listen_word)
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
            ), callback='parse_item', follow=follow, cb_kwargs={"dont_filter": dont_filter, "listen_word": listen_word})
            # 新增rule
            if unique_id not in self.rule_id:
                self.rule_id.append(unique_id)
                self._rules.append(rule)
                self._rules[-1]._compile(self)
            # 更新rule
            else:
                index = self.rule_id.index(unique_id)
                self._rules[index] = rule
                self._rules[index]._compile(self)
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
            self.rule_id = new_rule_id

    def _build_request(self, rule_index, link):
        return Request(
            url=link.url,
            callback=self._callback,
            errback=self._errback,
            meta=dict(rule=rule_index, link_text=link.text),
            dont_filter=self._rules[rule_index].cb_kwargs.get("dont_filter", False),
        )

    def parse_item(self, response, **kwargs):
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

        self.listen(response, kwargs)
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

    def listen(self, response, kwargs):
        # 监听关键词
        request = response.request
        refer = request.headers.get("Referer", b"").decode()
        request_url = response.url
        meta = response.meta
        link_text = meta.get("link_text")
        depth = meta.get("depth")
        download_slot = meta.get("download_slot")

        listen_word = kwargs.get("listen_word")
        if listen_word:
            org_listen_word = listen_word[7:-5]
            pa1 = re.compile(r'.*?(?=' + org_listen_word + r').*')
            pa2 = re.compile(listen_word)
            find_in_extra = pa1.findall(request_url) or pa1.findall(link_text) or pa1.findall(refer)
            find_in_text = pa2.findall(response.text)
            # mailer = MailSender.from_settings(self.settings)
            # mailer.send(to=["910804316@qq.com"], subject="Some subject", body="Some body", cc=["910804316@qq.com"])
            if find_in_extra or find_in_text:
                record = {
                    "request_url": request_url,
                    "refer": refer,
                    "link_text": link_text,
                    "depth": depth,
                    "download_slot": download_slot,
                    "find_in_extra": list(set(find_in_extra)),
                    "find_in_content": list(set(find_in_text)),
                    "listen_word": org_listen_word
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
