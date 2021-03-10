import asyncio
import queue
import traceback
from threading import Thread
from aiohttp import ClientSession
from requests.adapters import ProxyError
from scrapy import signals
import requests

from havok_lib.spiders.downloader import Downloader, Request


class Box(object):
    def __init__(self, req):
        self.req = req
        self._resp = None
        self.need_to_continue = False

    @property
    def resp(self):
        return self._resp

    @resp.setter
    def resp(self, resp):
        self._resp = resp


class CQueue(asyncio.Queue):
    def __init__(self, maxsize=0, *, loop=None, maxinput=None, spiders=None):
        super(CQueue, self).__init__(maxsize=maxsize, loop=None)
        self.maxinput = maxinput
        self.spider_count = {spider.name: 0 for spider in spiders}

    @asyncio.coroutine
    def put(self, item):
        if self.maxinput:
            spider = getattr(item, "spidername", None)
            if spider:
                if self.spider_count[spider] < self.maxinput:
                    self.spider_count[spider] += 1
                    return super(CQueue, self).put(item)
        else:
            return super(CQueue, self).put(item)


class TQueue(queue.Queue):
    def __init__(self, maxsize=0, maxinput=None, spiders=None):
        super(TQueue, self).__init__(maxsize=maxsize)
        self.maxinput = maxinput
        self.spider_count = {spider.name: 0 for spider in spiders}

    def put(self, item, block=True, timeout=None):
        if self.maxinput:
            spider = getattr(item, "spidername", None)
            if spider:
                if self.spider_count[spider] < self.maxinput:
                    self.spider_count[spider] += 1
                    return super(TQueue, self).put(item)
        else:
            return super(TQueue, self).put(item)


class CrawlMech(object):
    def __init__(self, engine):
        self.engine = engine
        self.spiders = engine.spiders
        self.spiders_dict = engine.spiders_dict
        self.concurrency = engine.concurrency

    def crawl_spiders(self):
        raise NotImplemented

    def run(self):
        raise NotImplemented

    def close(self):
        pass

    def shutdown(self):
        pass


class CoroCrawlMech(CrawlMech):
    def __init__(self, engine, maxpages):
        super(CoroCrawlMech, self).__init__(engine)
        self.loop = asyncio.get_event_loop()
        self.session = ClientSession(loop=self.loop)
        self.download_q = CQueue(loop=self.loop, maxinput=maxpages, spiders=self.spiders)
        self.dl = Downloader(loop=self.loop)

    def shutdown(self):
        """
        一旦关闭，将不能再重启服务
        :return:
        """
        self.loop.close()

    def close(self):
        self.session.close()
        self.dl.close()

    def run(self):
        self.loop.run_until_complete(self.crawl_spiders())

    async def schedule_request_worker(self):
        while True:
            req = await self.download_q.get()
            if req is None:
                # None is the signal to stop.
                self.download_q.task_done()
                break
            else:
                spider = self.engine.get_spider(req)
                box = Box(req)
                await self.dl.download_handle_coro(box, spider, self.download_q, self.engine.store_item)
                if box.need_to_continue:
                    self.download_q.task_done()
                    continue
                response = box.resp
                spider.signals.send_catch_log(signal=signals.response_received, response=response, request=box.req, spider=spider)

                # 处理爬虫输出
                for obj in await self.asyn_spider_parse(spider, response):
                    if isinstance(obj, Request):
                        await self.schedule_request_asyn(obj, spider)
                    else:
                        self.engine.store_item(obj, spider, response)
                self.download_q.task_done()

    async def crawl_spiders(self):
        consumers = [self.loop.create_task(self.schedule_request_worker()) for _ in range(self.concurrency)]

        async def producer():
            for spider in self.spiders:
                for request in spider.get_start_requests():
                    await self.schedule_request_asyn(request, spider)
            await self.download_q.join()
            for c in consumers:
                c.cancel()

        producers = [self.loop.create_task(producer())]
        # res = await asyncio.gather(*(consumers + producers), loop=self.loop, return_exceptions=True)
        # print(res)
        res, pending = await asyncio.wait(consumers + producers, loop=self.loop, return_when="FIRST_EXCEPTION")
        self.handle_exception(pending, res)

    async def schedule_request_asyn(self, req, spider):
        spider.signals.send_catch_log(signal=signals.request_scheduled,
                                      request=req, spider=spider)
        await self.download_q.put(req)

    async def asyn_spider_parse(self, spider, req):
        # parse = asyncio.coroutine(spider.parse)
        # return await asyncio.ensure_future(parse(request), loop=self.loop)
        parse = spider.dispatch_parse(req)
        if parse:
            return map(spider.ensure_good_request, await self.loop.run_in_executor(None, parse, req))

    def handle_exception(self, pending, res):
        for task in res:
            spider = self.spiders[0]
            try:
                stacks = task.get_stack()

                for stack in stacks:
                    resp = stack.f_locals.get("resp") or stack.f_locals.get("response")
                    req = stack.f_locals.get("req") or getattr(resp, "request", None)
                    temp_spider = stack.f_locals.get("spider") or self.spiders_dict.get(getattr(req, "spidername", ""))
                    if temp_spider:
                        spider = temp_spider
                        break

                excp = task.exception()

                if excp:
                    raise excp
            except asyncio.CancelledError:
                continue
            except:
                if spider:
                    spider.logger.error(traceback.format_exc())
                else:
                    print("Can't find spider in namespace!")
                    traceback.print_exc()
        for task in pending:
            task.cancel()


class ThreadCrawlMech(CrawlMech):
    def __init__(self, engine, maxpages):
        super(ThreadCrawlMech, self).__init__(engine)
        self.download_q = TQueue(maxinput=maxpages, spiders=self.spiders)
        self.dl = Downloader()

    def close(self):
        self.dl.close()

    def run(self):
        self.crawl_spiders_thread()

    def schedule_request(self, req, spider):
        spider.signals.send_catch_log(signal=signals.request_scheduled,
                                      request=req, spider=spider)
        self.download_q.put(req)

    def crawl_spiders_thread(self):
        def worker():
            while 1:
                req = self.download_q.get()
                try:
                    if req is None:
                        # None is the signal to stop.
                        self.download_q.task_done()
                        break
                    else:
                        spider = self.engine.get_spider(req)
                        box = Box(req)
                        self.dl.download_handle_thread(box, spider, self.download_q, self.engine.store_item)
                        if box.need_to_continue:
                            self.download_q.task_done()
                            continue
                        response = box.resp
                        spider.signals.send_catch_log(signal=signals.response_received, response=response,
                                                      request=box.req, spider=spider)

                        parse = spider.dispatch_parse(response)
                        if not parse:
                            continue
                        objs = parse(response)
                        for obj in objs:
                            if isinstance(obj, Request):
                                self.schedule_request(spider.ensure_good_request(obj), spider)
                            elif obj is None:
                                continue
                            else:
                                self.engine.store_item(obj, spider, response)
                        self.download_q.task_done()
                except:
                    spider = self.engine.get_spider(req)
                    spider.logger.error(traceback.format_exc())
                    self.download_q.task_done()

        workers = [Thread(target=worker) for _ in range(self.concurrency)]

        for spider in self.spiders:
            for req in spider.get_start_requests():
                self.schedule_request(req, spider)

        for w in workers:
            w.start()
        self.download_q.join()

        for i in range(len(workers)):
            self.download_q.put(None)


class SynCrawlMech(CrawlMech):
    def __init__(self, engine, maxpages):
        super(SynCrawlMech, self).__init__(engine)
        self.dl = Downloader()

    def run(self):
        return self.crawl_spiders_sync()

    def crawl_spiders_sync(self):
        assert len(self.spiders) < 2, "not supporting multispiders so far"
        next_to_be_sent = []
        for spider in self.spiders:
            logger = spider.logger
            try:
                this_to_be_sent = spider.get_start_requests()
                while this_to_be_sent:
                    for req in this_to_be_sent:
                        spider.signals.send_catch_log(signal=signals.request_scheduled, request=req, spider=spider)
                        box = Box(req)
                        for i in self.dl.download_handle_sync(box, spider, next_to_be_sent, self.engine.store_item):
                            yield i
                        if box.need_to_continue:
                            continue
                        response = box.resp
                        spider.signals.send_catch_log(signal=signals.response_received, response=response,
                                                      request=box.req, spider=spider)

                        parse = spider.dispatch_parse(response)
                        if not parse:
                            continue
                        objs = parse(response)
                        for obj in objs:
                            if isinstance(obj, Request):
                                spider.signals.send_catch_log(signal=signals.request_scheduled, request=obj, spider=spider)
                                next_to_be_sent.append(spider.ensure_good_request(obj))
                            elif obj is None:
                                continue
                            else:
                                self.engine.store_item(obj, spider, req)
                                yield obj
                    this_to_be_sent = next_to_be_sent
                    next_to_be_sent = []
            except ProxyError as pr:
                logger.error(traceback.format_exc())
                raise pr
            except requests.exceptions.ConnectTimeout as to:
                logger.error(traceback.format_exc())
                raise to
            except requests.exceptions.ConnectionError as cn:
                logger.error(traceback.format_exc())
                raise cn
            except:
                logger.error(traceback.format_exc())

