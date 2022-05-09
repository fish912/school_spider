import time
import datetime
from abc import ABC
from dateutil import parser
import scrapy
import hashlib
from scrapy_redis.spiders import RedisCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from selectolax.parser import HTMLParser
from urllib.parse import urlparse
from school.items import SchoolItem
import re
from school.database.redis_db import REDIS
from school.database.mongo import MONGO
from scrapy.http import Request
# from scrapy.mail import MailSender
from threading import Thread
from scrapy.signalmanager import SignalManager
from scrapy.utils.response import get_base_url
from scrapy.utils.python import unique as unique_list
from w3lib.url import canonicalize_url


class MyLinkExtractor(LinkExtractor):
    def __init__(self, red=None, *args, **kwargs):
        super(MyLinkExtractor, self).__init__(*args, **kwargs)
        self.red = red

    def extract_links(self, response):
        base_url = get_base_url(response)
        if self.restrict_xpaths:
            docs = [
                subdoc
                for x in self.restrict_xpaths
                for subdoc in response.xpath(x)
            ]
        else:
            docs = [response.selector]
        all_links = []
        for doc in docs:
            links = self._extract_links(doc, response.url, response.encoding, base_url)
            all_links.extend(self._process_links(links, response.url))
        return unique_list(all_links)

    def _process_links(self, links, from_url=None):
        new_links = []
        not_allow_link_url = []
        for x in links:
            if self._link_allowed(x):
                new_links.append(x)
            else:
                not_allow_link_url.append(x.url)

        if self.canonicalize:
            for link in new_links:
                link.url = canonicalize_url(link.url)
        new_links = self.link_extractor._process_links(new_links)
        return new_links


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
        key_patterns = []
        for i in listen_word:
            key_pattern = r'<.*?(?=' + i.strip() + r').*?>'
            key_patterns.append(re.compile(key_pattern))
        return key_patterns

    def __init_rule(self):
        for one in MONGO.get_rule_filter_by_id([]):
            unique_id = one.get("unique_id")
            if unique_id not in self.rule_id:
                self.rule_id.append(unique_id)
                follow = one.get("follow")
                listen_word = one.get("listen_word")
                if listen_word:
                    listen_word = self.__init_listen_word(listen_word)
                rule = Rule(MyLinkExtractor(
                    allow=tuple(one.get("allow", ())),
                    deny=tuple(one.get("deny", ())),
                    allow_domains=tuple(one.get("allow_domains", ())),
                    deny_domains=tuple(one.get("deny_domains", ())),
                    red=self.red
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
            rule = Rule(MyLinkExtractor(
                allow=allow,
                deny=deny,
                allow_domains=allow_domains,
                deny_domains=deny_domains,
                red=self.red
            ), callback='parse_item', follow=follow,
                cb_kwargs={"dont_filter": dont_filter, "listen_word": listen_word})
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
        host = urlparse(response.url).netloc.lower()

        self.listen(response, kwargs, host, url_fingerprint, title)
        item = SchoolItem()
        item['url'] = response.url
        item['url_fingerprint'] = url_fingerprint
        item['title'] = title
        item['content'] = text
        item['html_fingerprint'] = html_fingerprint
        item['host'] = host
        item['id'] = url_fingerprint

        not_update = MONGO.exist_finger(url_fingerprint, html_fingerprint, text, response.url)
        if not_update:
            return
        return item

    def listen(self, response, kwargs, host, url_fingerprint, title):
        # 监听关键词
        request = response.request
        refer = request.headers.get("Referer", b"").decode()
        request_url = response.url
        meta = response.meta
        link_text = meta.get("link_text")
        depth = meta.get("depth")
        download_slot = meta.get("download_slot")

        listen_word = kwargs.get("listen_word") or []
        if listen_word:
            find_in_text = list()
            find_word = list()
            for i in listen_word:
                find_res = i.findall(response.text)
                if find_res:
                    find_in_text.extend(find_res)
                    find_word.append(i.pattern[7:-5])
            if find_in_text:
                self.red.sadd("selenium_url", request_url)
                record = {
                    "request_url": request_url,
                    "url_fingerprint": url_fingerprint,
                    "refer": refer,
                    "link_text": link_text,
                    "depth": depth,
                    "download_slot": download_slot,
                    "find_in_content": list(set(find_in_text)),
                    "listen_word": find_word,
                    "title": title,
                    "host": host,
                    "update_time": parser.parse(str(datetime.datetime.now()))
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
