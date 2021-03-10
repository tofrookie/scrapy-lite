from havok_lib.spiders.settings import RETRY_CODES, NEED_RETRY, MAX_RETRY_TIMES, BAD_CODES
import re

class RetryMiddleware(object):
    def process_response(self, response, spider):
        req = response.request
        if req.status_code in RETRY_CODES:
            if NEED_RETRY:
                retry_times = req.meta.setdefault("retry_times", 0)
                if retry_times < MAX_RETRY_TIMES:
                    req.meta["retry_times"] += 1
                    spider.logger.info("[Retry: %s]" % req.url)
                    return req
                else:
                    spider.logger.error("[Retry too many times: %s]" % req.url)
            else:
                item = dict()
                item["exception"] = (True, req.status_code, req.reason)

                if req.meta.get("url_index") is not None:
                    item["url_index"] = req.meta.get("url_index")
                if req.meta.get("cid_index") is not None:
                    item["cid_index"] = req.meta.get("cid_index")
                if req.meta.get("uid") is not None:
                    if req.spidername == "follower":
                        item["followee_id"] = req.meta.get("uid")
                    elif req.spidername == "followee":
                        item["follower_id"] = req.meta.get("uid")
                    else:
                        item["user_id"] = req.meta.get("uid")
                if req.meta.get("keyword") is not None:
                    item["keyword"] = req.meta.get("keyword")
                return item


class BadRespMiddleware(object):
    def process_response(self, response, spider):
        req = response.request
        if re.search('https?://club.autohome.com.cn/bbs/thread/\w+', req.url):
            return

        if req.status_code in BAD_CODES:
            item = dict()
            item["exception"] = (True, req.status_code, req.reason)
            if req.meta.get("url_index") is not None:
                item["url_index"] = req.meta.get("url_index")
            if req.meta.get("cid_index") is not None:
                item["cid_index"] = req.meta.get("cid_index")
            if req.meta.get("uid") is not None:
                if req.spidername == "follower":
                    item["followee_id"] = req.meta.get("uid")
                elif req.spidername == "followee":
                    item["follower_id"] = req.meta.get("uid")
                else:
                    item["user_id"] = req.meta.get("uid")
            if req.meta.get("keyword") is not None:
                item["keyword"] = req.meta.get("keyword")
            return item


class TimeoutMiddleware(object):
    def process_response(self, response, spider):
        req = response.request
        if req.reason == "timeout":
            item = dict()
            item["exception"] = (True, 0, req.reason)
            if req.meta.get("url_index") is not None:
                item["url_index"] = req.meta.get("url_index")
            if req.meta.get("cid_index") is not None:
                item["cid_index"] = req.meta.get("cid_index")
            if req.meta.get("uid") is not None:
                if req.spidername == "follower":
                    item["followee_id"] = req.meta.get("uid")
                elif req.spidername == "followee":
                    item["follower_id"] = req.meta.get("uid")
                else:
                    item["user_id"] = req.meta.get("uid")
            if req.meta.get("keyword") is not None:
                item["keyword"] = req.meta.get("keyword")
            return item