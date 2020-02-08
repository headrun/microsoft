import MySQLdb
from urllib.parse import urlencode

from scrapy import signals
import re
from scrapy.http import FormRequest,Request
import scrapy
import datetime
import requests
import json


class MicrosoftPapers(scrapy.Spider):
    name = 'microsoft_papers'
    handle_httpstatus_list = [404.500]
    allowed_domains = ['academic.microsoft.com']

    def __init__(self, *args, **kwargs):
        super(MicrosoftPapers, self).__init__(*args, **kwargs)
        self.topic_name = kwargs.get('topic_name', '')
        self.values_list = []
        self.references_list = []
        self.from_ = kwargs.get('from','')
        self.to_ = kwargs.get('to','')
        self.cursor = MySQLdb.connect(host='localhost',user='root',passwd='e3e2b51caee03ee85232537ccaff059d167518e2',db='MICROSOFTPAPERSDB',charset='utf8', use_unicode=True).cursor()
        self.insert_query = "insert into papers_info_table(sk,title,topic,description,category,entity_type,keywords,authors,main_reference_url,paper_status,created_at,modified_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) on duplicate key update modified_at = now();"
        self.insert_query1 = "insert into reference_papers(sk,reference_url,main_reference_url,status,created_at,modified_at) values(%s,%s,%s,%s,now(),now()) on duplicate key update modified_at = now();"
        self.insert_topic_query = "insert into parent_papers_info(sk,topic,reference_url,created_at,modified_at) values(%s,%s,%s,now(),now()) on duplicate key update modified_at = now();"
        #self.main()



    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MicrosoftPapers, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self,spider,reason):
        self.cursor.executemany(self.insert_query,self.values_list)
        self.cursor.executemany(self.insert_query1,self.references_list)
        self.cursor.executemany(self.insert_topic_query,self.topic_values_list)

    def get_headers(self,req_url):
        headers = {
            'origin': 'https://academic.microsoft.com',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/66.0.3359.181 Chrome/66.0.3359.181 Safari/537.36',
            'content-type': 'application/json; charset=utf-8',
            'accept': 'application/json',
            'referer': req_url,
            'authority': 'academic.microsoft.com',
            'x-requested-with': 'Fetch',
            'dnt': '1',
        }
        return headers

    def start_requests(self):
        if self.topic_name:
            if self.from_ and self.to_:
                for i in range(int(self.from_),int(self.to_)):
                    from_ = int(i)
                    to_  =  i+1
                    req_url = 'https://academic.microsoft.com/search?q={}&f=&eyl=Y%3C%3D{}&syl=Y%3E%3D{}&orderBy=4'.format(self.topic_name,str(to_),str(from_))
                    data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":["Y>=%s","Y<=%s"]' %(str(from_),str(to_)) + ',"orderBy":4,"skip":0,"take":10}'
                    headers = self.get_headers(req_url)
                    yield scrapy.Request('https://academic.microsoft.com/api/search',method="POST",body= data,headers=headers,callback=self.parse,meta={'data':data,'req_url':req_url,'crawlable_check':True})
            elif self.from_ or self.to_:
                if self.to_:  
                    req_url = 'https://academic.microsoft.com/search?q={}&f=&eyl=Y%3C%3D{}&orderBy=4'.format(self.topic_name,self.to_)
                    data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":["Y<=%s"]' %(self.to_) + ',"orderBy":4,"skip":0,"take":10}'
                if self.form_: 
                    req_url = 'https://academic.microsoft.com/search?q={}&f=&eyl=Y%3E%3D{}&orderBy=4'.format(self.topic_name,self.from_)
                    data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":["Y>=%s"]' %(self.to_) + ',"orderBy":4,"skip":0,"take":10}'
            else:
                req_url = 'https://academic.microsoft.com/search?q=%s&f=&orderBy=4' %self.topic_name
                data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":[],"orderBy":4,"skip":0,"take":10}'
        else:
            print("please provide topic name")
        #headers = self.get_headers(req_url)
        #yield scrapy.Request('https://academic.microsoft.com/api/search',method="POST",body= data,headers=headers,callback=self.parse,meta={'data':data,'req_url':req_url,'crawlable_check':True})
        
        #data = '{"query":' + "%s" % str(self.topic_name)+ ',"queryExpression":"","filters":[],"orderBy":0,"skip":0,"sortAscending":true,"take":10,"includeCitationContexts":false}'
        #data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":[],"orderBy":4,"skip":0,"sortAscending":true,"take":10,"includeCitationContexts":false}'
        #try:
            #response = requests.post('https://academic.microsoft.com/api/search', headers=headers, data=data)
    def parse(self,response):
        req_url = response.meta.get('req_url','')
        resp_ = json.loads(response.body)
        pagenavigation_count = resp_.get('t','')
        crawlable_check = response.meta.get('crawlable_check',False)
        #self.get_papers(resp_)
        '''papers_list = resp_.get('pr',[])
        pagenavigation_count = resp_.get('t','')
        for pr_dict in papers_list:
            paper = pr_dict.get('paper',{})
            id_ = paper.get('id','')
            title = paper.get('dn','')
            description = paper.get('d','')
            entity_type= paper.get('et','')
            tag_lines = paper.get('fos',[])
            tag_list = [i.get('dn','') for i in tag_lines if i.get('dn','')!='']
            try: category = paper.get('v',{}).get('displayName','')
            except: category = ''
            reference_url = 'https://academic.microsoft.com/paper/' + str(id_)
            headers = self.get_headers(reference_url)
            params = {'entityType': entity_type}
            #try:
            values = [str(id_),str(title),str(self.topic_name),str(description),str(category),str(entity_type),str(tag_list),reference_url]
            self.values_list.append(values)
            if len(self.values_list) > 500:
                #insert_query += str(values)
                #insert_query += ' on duplicate key update modified_at = now()'
                self.cursor.executemany(self.insert_query,self.values_list)
                self.values_list = []
            api_url = 'https://academic.microsoft.com/api/entity/' + str(id_) + '?' + urlencode(params)
            #api_url = api_url + '?' + urlencode(params)
            yield scrapy.Request(api_url, headers=headers,callback=self.get_reference_links,meta={'data':values,'req_url':reference_url})'''


        if crawlable_check:
            data = response.meta.get('data','')
            split_url = req_url.split('&skip=')
            for i in range(0,pagenavigation_count,10):
                req_url = split_url[0] +'&skip='+ str(i) + '&take=10'
                #req_url = req_url + '&skip=%s&take=10' %(i)
                #data.update({'skip':str(i)})
                dat_ = data.split(',')
                skip = data.split(',')[-2]
                skip_after = '"skip":%s' %i
                dat_[-2] = data.split(',')[-2].replace(skip,skip_after)
                print (dat_)

                #data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":[],"orderBy":4,"skip":' + str(i) + ',"sortAscending":true,"take":10,"includeCitationContexts":false}'
                headers = self.get_headers(req_url)
                yield scrapy.Request('https://academic.microsoft.com/api/search',method="POST",body= ','.join(dat_),headers=headers,callback=self.get_papers,meta={'data':','.join(dat_),'req_url':req_url})
                #respon = requests.post('https://academic.microsoft.com/api/search', headers=headers, data=data)
                '''try:
                    respo_ = json.loads(respon.text)
                except:
                    continue
                self.get_papers(respo_)
        #except:
        #pass'''
    
    def get_papers(self,response):
        response_ = json.loads(response.body)
        #pag_navigation = response.get('')
        papers_list = response_.get('pr',[])
        pagenavigation_count = response_.get('t','')
        for pr_dict in papers_list[:1]:
            paper = pr_dict.get('paper',{})
            id_ = paper.get('id','')
            title = paper.get('dn','')
            description = paper.get('d','')
            entity_type= paper.get('et','')
            tag_lines = paper.get('fos',[])
            tag_list = [i.get('dn','') for i in tag_lines if i.get('dn','')!='']
            try: category = paper.get('v',{}).get('displayName','')
            except: category = ''
            reference_url = 'https://academic.microsoft.com/paper/' + str(id_)
            headers = self.get_headers(reference_url)
            params = {'entityType': entity_type}
            #try:
            values = [str(id_),str(title),str(self.topic_name),str(description),str(category),str(entity_type),str(tag_list),reference_url]
            '''self.values_list.append(values)
            if len(self.values_list) > 500:
                #insert_query += str(values)
                #insert_query += ' on duplicate key update modified_at = now()'
                self.cursor.executemany(self.insert_query,self.values_list)
                self.values_list = []'''
            api_url = 'https://academic.microsoft.com/api/entity/' + str(id_) + '?' + urlencode(params)
            #api_url = api_url + '?' + urlencode(params)
            yield scrapy.Request(api_url, headers=headers,callback=self.get_reference_links,meta={'data':values,'req_url':reference_url})
            #enti_res = json.loads(enti_res.text)
            #except: pass'''


    def get_reference_links(self,response):
        enti_res = json.loads(response.body)
        data = response.meta.get('data',[])
        id_,title,topic_name,description,category,entity_type,tag_list,reference_url = data
        entity_dict = enti_res.get('entity',{})
        if entity_dict:
            authors = entity_dict.get('a',[])
            authors_list = [k.get('dn','') for k in authors if k.get('dn','')!='']
        #reference_link = enti_res.get('paperReferencesExpression','')

        #if reference_link:
        reference_ids_list = entity_dict.get('r',[])
        #else:
        #reference_ids_list = []
        related_ids_list = entity_dict.get('rp',[])
        for k in related_ids_list:
            reference_link_format_ = 'https://academic.microsoft.com/paper/%s' %(k)
            self.references_list.append((str(k),str(reference_link_format_),str(reference_url),'0'))
        for i in reference_ids_list:
            reference_link_format = 'https://academic.microsoft.com/paper/%s' %(i)
            self.references_list.append((str(i),str(reference_link_format),str(reference_url),'0'))
        cited_by = enti_res.get('citedByExpression','')
        if cited_by:
            headers = self.get_headers(reference_url)
            data = '{"query":"%s","queryExpression":"%s","filters":[],"orderBy":0,"skip":0,"take":10,"parentEntityId":%s,"profileId":""}' %(title,str(cited_by),id_)
            yield scrapy.Request('https://academic.microsoft.com/api/search',method="POST",body= data,headers=headers,callback=self.parse_citedby,meta = {'headers':headers,'data':data,'ref_url':reference_url,'crawlable_check':True})
            

        #insert_query = "insert into papers_info_table('sk','title','topic','description','category','entity_type','keywords','authors','reference_ids','reference_link_format','main_reference_url','created_at','modified_at') values"
        values = (str(id_),str(title),str(topic_name),str(description),str(category),str(entity_type),str(tag_list),str(authors_list),reference_url,'main_paper')
        #insert_query += str(values)
        self.values_list.append(values)
        if id_: self.topic_values_list.append((str(id_),self.topic_name,reference_url))
        if len(self.topic_values_list) > 500:
            self.cursor.executemany(self.insert_topic_query,self.topic_values_list)
            self.topic_values_list = []
        if len(self.values_list) > 500:
            self.cursor.executemany(self.insert_query,self.values_list)
            self.values_list = []
        if len(self.references_list) > 500:
            self.cursor.executemany(self.insert_query1,self.references_list)
            self.references_list = []
        #insert_query += ' on duplicate key update modified_at = now()'
        #self.cursor.execute(self.insert_query,values)
        
            
    def parse_citedby(self,response):
        json_data = json.loads(response.body)
        crawlable_check = response.meta.get('crawlable_check',False)
        related_ids_list = json_data.get('pr',[])
        ref_url = response.meta.get('ref_url','')
        pagenavigation_count = json_data.get('t','')
        if crawlable_check:
            data = eval(response.meta.get('data',''))
            split_url = ref_url.split('&skip=')
            for i in range(0,pagenavigation_count,10):
                #data =  split_url[0] +'&skip='+ str(i) + '&take=10'
                #req_url = req_url + '&skip=%s&take=10' %(i)
                #data.update({'skip':str(i)})
                #dat_ = data.split(',')
                #skip = data.split(',')[-2]
                #skip_after = '"skip":%s' %i
                #dat_[-2] = data.split(',')[-2].replace(skip,skip_after)
                #print (dat_)

                #data = '{"query":' + '"%s"' % str(self.topic_name)+ ',"queryExpression":"","filters":[],"orderBy":4,"skip":' + str(i) + ',"sortAscending":true,"take":10,"includeCitationContexts":false}'
                if isinstance(data,dict):
                    data.update({'skip':i})
                    headers = self.get_headers(ref_url)
                    yield scrapy.Request('https://academic.microsoft.com/api/search',method="POST",body= json.dumps(data),headers=headers,callback=self.parse_citedby,meta={'data':json.dumps(data),'ref_url':ref_url,'headers':headers,'crawlable_check':False})
                    #respon = requests.post('https://academic.microsoft.com/api/search', headers=headers, data=data)
                    
        for k in related_ids_list:
            paper = k.get('paper',{})
            id_ = paper.get('id','')
            reference_link_format_ = 'https://academic.microsoft.com/paper/%s' %(id_)
            self.references_list.append((str(id_),str(reference_link_format_),str(ref_url),'0'))

            
