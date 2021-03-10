

from scrapy_lite.base_spider import *


class TestSpider(BaseSpider):
    """
    测试
    """
    name = "test_spider"

    def __init__(self, url, *args, **kwargs):

        self.start_urls = [url]

    def parse(self, response):

        yield response


if __name__ == '__main__':
    url = 'http://www.baidu.com'
    se = SpiderEngine(TestSpider(url), async=False)

    for item in se.sync_run():
        print(item)