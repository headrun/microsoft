import MySQLdb
from urllib.parse import urlencode
import re
from scrapy.http import FormRequest,Request
import scrapy
import datetime
import requests
import json
from scrapy import signals



class MicrosoftReference(scrapy.Spider):
    name = 'microsoft_reference'
    allowed_domains = ['academic.microsoft.com']

    def __init__(self, *args, **kwargs):
        super(MicrosoftReference, self).__init__(*args, **kwargs)
        self.values_list = []
        self.reference_list = []
        self.references_list = []
        self.update_values_list = []
        self.insert_query = "insert into papers_info_table(sk,title,topic,description,category,entity_type,keywords,authors,reference_ids,reference_link_format,main_reference_url,paper_status,created_at,modified_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) on duplicate key update modified_at = now();"
        self.insert_query1 = "insert into reference_papers(sk,reference_url,main_reference_url,status,created_at,modified_at) values(%s,%s,%s,%s,now(),now()) on duplicate key update modified_at = now();"
        self.conn = MySQLdb.connect(host='localhost',user='root',passwd='root',db='MICROSOFTPAPERSDB',charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MicrosoftReference, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
 
    def _spider_closed(self, spider, reason):
        self.cursor.executemany(self.insert_query,self.values_list)
        self.update_sks_to_1(self.update_values_list)



    def spider_closed(self,spider,reason):
        self.cursor.executemany(self.insert_query,self.values_list)
        self.update_sks_to_1(self.update_values_list)


    def start_requests(self):
        select_query = 'select sk,reference_url,main_reference_url from reference_papers where status=0 order by rand() limit 1000'
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        row_ = [(i[0],) for i in rows]
        for row in rows:
            reference_url = row[1]
            sk = row[0]
            headers = self.get_headers(reference_url)

            url1 = 'https://academic.microsoft.com/api/entity/' + str(sk) + '?entityType=2'
            yield scrapy.Request(url1, headers=headers,callback=self.parse, meta={'sk':sk,'req_url':reference_url})

    def update_sks_to_9(self,row_):
        update_query = "update ignore reference_papers set status=9 where sk= %s"

        self.cursor.executemany(update_query,row_)
        self.conn.commit()
        return

    def update_sks_to_1(self,row_):
        update_query = "update ignore reference_papers set status=1 where sk in %s"
        self.cursor.execute(update_query %str(tuple(row_)))
        self.conn.commit()
        return


    def get_headers(self, req_url):
        headers = {
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/66.0.3359.181 Chrome/66.0.3359.181 Safari/537.36',
                    'accept': 'application/json',
                    'referer': req_url,
                    'authority': 'academic.microsoft.com',
                    'x-requested-with': 'Fetch',
                    'dnt':'1',
                        }
        return headers

    def parse(self, response):
        req_url = response.meta.get('req_url')
        sk = response.meta.get('sk')
        res = json.loads(response.body)
        if '/paper/' in req_url:
            sk = res.get('entity').get('id')
            title = res.get('entity').get('dn')
            description = res.get('entity').get('d')
            entity_dict = res.get('entity')
            if entity_dict:
                authors = entity_dict.get('a',[])
                authors_list = [k.get('dn','') for k in authors if k.get('dn','')!='']
            key = entity_dict.get('fos')
            keywords = [k.get('dn','') for k in key if k.get('dn','')!='']
            entity_type = res.get('entityType')
            reference_paper = entity_dict.get('r',[])
            related_papers = entity_dict.get('rp',[])
            if 'Biology' in keywords or 'biology' in keywords:
                topic_name = 'biology'
            else:
                topic_name = ''
            category = ''
            reference_link_format = 'https://academic.microsoft.com/paper/{reference_id}'
            values = (str(sk), str(title),str(topic_name),str(description),str(category),str(entity_type),str(keywords),str(authors_list), str(reference_paper),reference_link_format,req_url,'reference_paper')
            self.values_list.append(values)
            self.update_values_list.append(sk)
            for k in related_papers:
            	reference_link_format_ = 'https://academic.microsoft.com/paper/%s' %(k)
            	self.references_list.append((str(k),str(reference_link_format_),str(req_url),'0'))
            for pa in reference_paper:
                reference_link_format = 'https://academic.microsoft.com/paper/%s' %(pa)
                self.references_list.append((str(pa),str(reference_link_format),str(req_url),'0'))
            if len(self.values_list) > 500:
                self.cursor.executemany(self.insert_query,self.values_list)
                self.update_sks_to_1(self.update_values_list)
                self.update_values_list = []
                self.values_list=[]
            if len(self.references_list) > 500:
                self.cursor.executemany(self.insert_query1,self.references_list)
                self.references_list = []
            

