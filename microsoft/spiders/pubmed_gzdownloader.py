from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from microsoft.items import MicrosoftItem
from urllib.parse import urljoin

class PubmedDownloadSpider(Spider):
    name = 'pubmed_gzdownload'
    start_urls = ['https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/']

    def file_path(self, request, response=None, info=None):
        original_path = super(MyFilesPipeline, self).file_path(request, response=None, info=None)
        sha1_and_extension = original_path.split('/')[1] # delete 'full/' from the path
        return request.meta.get('filename','') + "_" + sha1_and_extension

    def parse(self, response):
        sel = Selector(response)
        test_item = MicrosoftItem()
        links = sel.xpath('//a[contains(@href, "xml")][not(contains(@href, "md5"))]/@href').extract()
        for link in links:
            if 'http' not in link:
                link = urljoin(response.url, link)
            test_item['file_url'] = link
            test_item['name'] = link.strip().split('/')[-1]
            yield test_item


