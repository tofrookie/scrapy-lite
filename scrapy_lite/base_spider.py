import functools
import traceback

import requests
from scrapy import signals
from scrapy.signalmanager import SignalManager

from scrapy_lite import NAME
from scrapy_lite.crawl_mechanisms import CoroCrawlMech, ThreadCrawlMech, SynCrawlMech
from scrapy_lite.downloader import Request
from scrapy_lite.settings import *
from scrapy_lite.logger import get_logger


class SpiderEngine(object):
    methods = ["coroutine", "thread"]

    def __init__(self, spiders, concurrency=16, maxpages=None, async=True, method="coroutine", *args, **kwargs):
        '''
        
        :param spiders:
        :param concurrency:
        :param maxpages:
        :param async:       [bool]true代表是异步。false代表同步
        :param method:
        :param args:
        :param kwargs:
        '''
        self.spiders = spiders if isinstance(spiders, list) else [spiders]
        self.spiders_dict = {spider.name: spider for spider in self.spiders}
        self.async = async
        self.concurrency = concurrency

        assert method in self.methods, "unknown method: %s, only %s are currently available!" % (method, self.methods)
        self.method = method
        if async:
            self.cm = CoroCrawlMech(self, maxpages) if method == "coroutine" else ThreadCrawlMech(self, maxpages)
        else:
            self.cm = SynCrawlMech(self, maxpages)

        self.started = False

    def open_spider(self, spider):
        spider.signals.send_catch_log(signals.spider_opened, spider=spider)

    def close_spider(self, spider, reason):
        spider.signals.send_catch_log(
            signal=signals.spider_closed, spider=spider, reason=reason)

    def get_log(self, request):
        spider = self.get_spider(request)
        return spider.logger

    def get_spider(self, request):
        return self.spiders_dict.get(request.spidername)

    def store_item(self, item, spider, response):
        if item:
            spider.logger.info("[scrap one item: %s]\nfrom [url: %s]" % (item, response.url))
            spider.storage.append(item)
            spider.signals.send_catch_log_deferred(
                signal=signals.item_scraped, item=item, response=response,
                spider=spider)

    def finish_up(self):
        for spider in self.spiders:
            logger = spider.logger
            logger.info("scraped %d items" % len(spider.storage))
            sgm = spider.signals
            sgm.send_catch_log(signal=signals.engine_stopped)
        self.cm.close()

    def start(self):
        self.started = True
        for spider in self.spiders:
            spider.signals.send_catch_log(signal=signals.engine_started)

    def run(self):
        assert self.async,  "not in async mode"
        self.start()
        try:
            self.cm.run()
        except KeyboardInterrupt:
            pass
        finally:
            self.finish_up()

    def sync_run(self):
        assert not self.async,  "not in sync mode"
        self.start()
        try:
            for i in self.cm.run():
                yield i
        except KeyboardInterrupt:
            pass
        finally:
            self.finish_up()

    def get_results(self):
        assert self.started, "Spiders haven't been started!"
        return {spider.name: spider.storage for spider in self.spiders}

    def shutdown(self):
        """
        一旦关闭，将不能再重启服务
        :return:
        """
        self.cm.shutdown()


class BaseSpider(object):
    name = ''
    start_urls = []

    def __init__(self, db=None,logpath="", proxy=None, is_proxy_http=False, headers=None, log = None,encoding=None, *args, **kwargs):
        # self.logger = logger(logpath + self.name + ".log" if not logpath else logpath).getLogger()
        
        self.logger = log
        if not log:
            self.logger = get_logger(self.name, log_root=logpath if logpath else DEFAULT_LOG_PATH + NAME, day_rotating=False)

        self.proxy = proxy
        self.is_proxy_http = is_proxy_http
        self.encoding = encoding
        self.headers = headers
        self.storage = []
        self.signals = SignalManager(self)
        self.logger.info(self.name + " start!")
        self.cookies = None
        self.db = db
        self.timeout = DEFAULT_TIMEOUT

        if self.db:
            res = self.db['cookies'].find_one({'spider': self.name})
            if res:
                self.cookies = res.get('cookie_str', '')

        self.get_start_requests = self.ensure_good_requests(self.get_start_requests)

    def make_request(self, url, *args, **kwargs):
        headers = kwargs.pop("headers", self.headers)
        return Request(url, proxy=self.proxy, is_proxy_http=self.is_proxy_http,headers=headers, spidername=self.name, timeout=self.timeout, encoding=self.encoding,*args, **kwargs)

    def ensure_good_request(self, obj):
        if isinstance(obj, Request):
            if not obj.spidername:
                obj.set_spidername(self.name)
        return obj

    def ensure_good_requests(self, funct):
        @functools.wraps(funct)
        def wrapper(*args, **kwargs):
            return [self.ensure_good_request(r) for r in funct(*args, **kwargs)]
        return wrapper

    def dispatch_parse(self, response):
        req = response.request
        parse = getattr(self, req.callback, None)
        if parse:
            return parse                # TODO：对request的预处理可以通过包上一层修饰器实现
        else:
            self.logger.error("[Wrong request with no callback: %s]" % req)

    def parse(self, request):
        raise NotImplemented

    def get_start_requests(self):
        start_requests = []
        for url in self.start_urls:
            request = self.make_request(url)
            start_requests.append(request)
        return start_requests


class SpiderMiddlewareManager(object):
    def __init__(self, spider):
        self.input_middlewares = []
        self.output_middlewares = []
        self.spider = spider

    def manage_spider_io(self, parse):
        @functools.wraps(parse)
        def func(req, *args, **kwargs):
            for mw in self.input_middlewares:
                req = mw.process_spider_input(req, self.spider)
            res = parse(req, *args, **kwargs)
            for mw in self.output_middlewares:
                res = mw.process_spider_output(req, res, self.spider)
            for obj in res:
                yield obj
        return func



class InvalidateUrlExcept(Exception):
    '''
    代表不符合规范的URL
    '''
    def __init__(self,url):
        super(InvalidateUrlExcept, self).__init__(url)
        self._err_url = url

    def __str__(self):
        return self._err_url
    

class MissRequiredFieldExcept(Exception):
    pass
    
    