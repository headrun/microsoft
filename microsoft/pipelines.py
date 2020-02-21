# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import gzip, os
from shutil import move
from scrapy.utils.project import get_project_settings
from scrapy.pipelines.files import FilesPipeline
from scrapy.http import Request

settings=get_project_settings()
dowload_path = settings.get('FILES_STORE', '')
extract_path = settings.get('FILES_EXTRACT', '')

class MyFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        return request.meta.get('filename','')

    def get_media_requests(self, item, info):
        file_url = item['file_url']
        meta = {'filename': item['name']}
        yield Request(url=file_url, meta=meta)

    def item_completed(self, results, item, info):
        for result in results:
            file_name = result[1].get('path', '')
            extracted_name = file_name.replace('.gz', '')
            os.system('gunzip -c %s > %s' % (os.path.join(dowload_path, file_name), os.path.join(extract_path, extracted_name)))

class MicrosoftPipeline(object):
    def process_item(self, item, spider):
        return item
