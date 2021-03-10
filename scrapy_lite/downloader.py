import asyncio

import requests
from aiohttp import ClientSession
from scrapy.http import TextResponse, Response

from havok_lib.spiders.middlerwares import TimeoutMiddleware, RetryMiddleware, BadRespMiddleware
from havok_lib.spiders.settings import DEFAULT_TIMEOUT


class DownloaderMiddlewareManager(object):
    def __init__(self):
        self.output_middlewares = [w() for w in [TimeoutMiddleware, RetryMiddleware, BadRespMiddleware]]
        self.input_middlewares = []
        self.exception_middlewares = []

    def manage_request_handle(self, req, spider):
        for mw in self.input_middlewares:
            obj = mw.process_request(req, spider)
            if obj:
                return obj

    def manage_response_handle(self, resp, spider):
        for mw in self.output_middlewares:
            obj = mw.process_response(resp, spider)
            if obj:
                return obj

    def manage_exception_handle(self, req, exception, spider):
        for mw in self.exception_middlewares:
            obj = mw.process_exception(req, exception, spider)
            if obj:
                return obj


class Downloader(object):
    def __init__(self, async=True, loop=None):
        self.async = async
        self.session = None
        if async and loop:
            self.session = ClientSession(loop=loop)
        self.dl_mwm = DownloaderMiddlewareManager()

    async def download_page(self, logger, req):
        info = ("[using proxy: %s] to" % req.proxy if req.proxy else "") + "[%s url: %s]" % (req.method, req.url)
        logger.info(info)
        try:
            # if not req.cookies:
            #     req.cookies = {}

            async with self.session.request(req.method, req.url, data=req.data, headers=req.headers, timeout=req.timeout,
                                            proxy=req.proxy) as r:
                text = await r.text(errors='replace')
                info = "[code: %s][status: %s][received: %s]" % (r.status, r.reason, req.url)

                logger.info(info)
                req.set_resp(text, str(r.url), r.status, r.reason)
        except asyncio.TimeoutError:
            req.set_resp("", status=408, reason="timeout")
        return self.produce_response(req)

    def download_page_sync(self, logger, req):
        if req.is_proxy_http:
            proxies = {"http": req.proxy} if req.proxy else None
        else:
            proxies = {"http": req.proxy, "https": req.proxy} if req.proxy else None

        info = ("[using proxy: %s] to " % req.proxy if req.proxy else "") + "[%s url: %s]" % (req.method, req.url)
        logger.info(info)
        try:
            cookies = req.cookies if req.cookies else {}

            resp = requests.request(req.method, req.url, data=req.data, headers=req.headers, timeout=req.timeout,
                                    proxies=proxies, cookies=cookies, verify=False)
            if req.encoding:
                resp.encoding = req.encoding

            logger.info("[code: %s][status: %s][received: %s]" % (resp.status_code, resp.reason, resp.url))
            if resp.status_code == 504:
                raise requests.exceptions.ConnectTimeout
            req.set_resp(resp.text, resp.url, resp.status_code, resp.reason)
        except requests.exceptions.ConnectTimeout:
            resp = req
            req.set_resp("", status=408, reason="timeout")
            logger.info("[status: timeout][url: %s]" % (resp.url))
            raise requests.exceptions.ConnectTimeout
        return self.produce_response(req)

    def produce_response(self, req):
        url, status, headers, body = req.url, req.status_code, req.headers, req.text
        return TextResponse(url=url, status=status, headers=headers, body=body, request=req, encoding='utf-8')

    async def download_handle_coro(self, box, spider, download_q, handle_store):
        assert getattr(self, "session"), "Loop not provided!"
        req = box.req
        resp = None

        req_obj = self.dl_mwm.manage_request_handle(req, spider)
        if isinstance(req_obj, Request):
            await download_q.put(req_obj)
        elif isinstance(req_obj, Response):
            resp = req_obj
        elif not req_obj:
            pass

        if not resp:
            resp = await self.download_page(spider.logger, req)

        resp_obj = self.dl_mwm.manage_response_handle(resp, spider)
        if isinstance(resp_obj, Request):
            await download_q.put(resp_obj)
            box.need_to_continue = True
        elif isinstance(resp_obj, Response):
            resp = req_obj
        elif not resp_obj:
            pass
        else:
            handle_store(resp_obj, spider, resp)
            box.need_to_continue = True
        box.resp = resp

    def download_handle_thread(self, box, spider, download_q, handle_store):
        req = box.req
        resp = None

        req_obj = self.dl_mwm.manage_request_handle(req, spider)
        if isinstance(req_obj, Request):
            download_q.put(req_obj)
        elif isinstance(req_obj, Response):
            resp = req_obj
        elif not req_obj:
            pass

        if not resp:
            resp = self.download_page_sync(spider.logger, req)

        resp_obj = self.dl_mwm.manage_response_handle(resp, spider)
        if isinstance(resp_obj, Request):
            download_q.put(resp_obj)
            box.need_to_continue = True
        elif isinstance(resp_obj, Response):
            resp = req_obj
        elif not resp_obj:
            pass
        else:
            handle_store(resp_obj, spider, resp)
            box.need_to_continue = True
        box.resp = resp

    def download_handle_sync(self, box, spider, next_to_be_sent, handle_store):
        req = box.req
        resp = self.download_page_sync(spider.logger, req)
        box.resp = resp

        req_obj = self.dl_mwm.manage_request_handle(req, spider)
        if isinstance(req_obj, Request):
            next_to_be_sent.append(req_obj)
        elif isinstance(req_obj, Response):
            resp = req_obj
        elif not req_obj:
            pass

        if not resp:
            resp = self.download_page_sync(spider.logger, req)

        resp_obj = self.dl_mwm.manage_response_handle(resp, spider)
        if isinstance(resp_obj, Request):
            next_to_be_sent.append(resp_obj)
            box.need_to_continue = True
        elif isinstance(resp_obj, Response):
            resp = req_obj
        elif not resp_obj:
            pass
        else:
            handle_store(resp_obj, spider, req)
            yield resp_obj
            box.need_to_continue = True
        box.resp = resp

    def close(self):
        if self.async and self.session:
            self.session.close()


class Request(object):
    def __init__(self, url, method="get", data=None, headers=None, cookies=None, timeout=DEFAULT_TIMEOUT, text=None, proxy=None,
                 is_proxy_http=None, meta=None, spidername=None, callback="parse",encoding=None, *args, **kwargs):

        self.url = url
        self.method = method
        self.headers = headers
        self.cookies = cookies
        self.timeout = timeout
        self.data = data
        self.text = text
        self.encoding = encoding
        self.callback = callback
        self.is_proxy_http = is_proxy_http

        if proxy is not None and not (proxy.startswith("http://") or proxy.startswith("https://")):
            proxy = "http://" + proxy
        self.proxy = proxy
        self.spidername = spidername
        self.meta = {}
        if meta:
            self.meta.update(meta)

        self.status_code = None
        self.reason = None

    def __str__(self):
        return "<%s %s>" % (self.method, self.url)

    __repr__ = __str__

    def set_resp(self, resp, url=None, status=None, reason=None):
        self.text = resp
        self.reason = reason
        self.status_code = status
        if url:
            self.url = url
        return self

    def set_spidername(self, name):
        self.spidername = name
        return self

    def copy(self):
        """Return a copy of this Request"""
        return self.replace()

    def replace(self, *args, **kwargs):
        """Create a new Request with the same attributes except for those
        given new values.
        """
        for x in ['url', 'method', 'headers', 'body', 'cookies', 'meta',
                  'encoding', 'priority', 'dont_filter', 'callback', 'errback']:
            kwargs.setdefault(x, getattr(self, x))
        cls = kwargs.pop('cls', self.__class__)
        return cls(*args, **kwargs)