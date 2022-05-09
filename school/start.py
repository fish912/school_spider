from scrapy import cmdline
from multiprocessing import Process
import hashlib
import time
from selenium import webdriver
from school.database.redis_db import REDIS
from selenium.webdriver.chrome.options import Options


def shortcut():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--window-size=1920x945')
    chrome_options.add_argument('--disable-gpu')
    browser = webdriver.Chrome("F:\webdriver\chromedriver.exe", chrome_options=chrome_options)
    browser.maximize_window()
    red = REDIS.rc
    while 1:
        url = red.spop("selenium_url")
        if not url:
            time.sleep(5)
            continue
        url_fingerprint = hashlib.md5(url).hexdigest()
        browser.get(url.decode('utf-8'))
        browser.implicitly_wait(6)
        width = browser.execute_script("return document.documentElement.clientWidth")
        height = browser.execute_script("return document.documentElement.clientHeight")
        browser.set_window_size(width, height)
        browser.save_screenshot(f'./pic/{url_fingerprint}.png')
        browser.stop_client()


if __name__ == '__main__':
    p = Process(target=shortcut)
    p.start()
    cmdline.execute("scrapy crawl school_spider".split())
    p.join()
