from os import path, getcwd, listdir
from shutil import move
from multiprocessing.pool import ThreadPool
from lxml.etree import parse
from MySQLdb import connect


class PubmedXMLParser(object):
    def __init__(self):
        self.values = []
        self.conn = connect(host='localhost', user='root', passwd='root', db='MICROSOFTPAPERSDB', charset='utf8')
        self.cursor = self.conn.cursor()
        self.to_read_path = path.join(getcwd(), 'OUTPUT/gz_extracted')
        self.to_move_path = path.join(getcwd(), 'OUTPUT/gz_processed')
        self.to_read_files = listdir(self.to_read_path)
        self.insert_query = 'insert into pubmed_data(id, doi, title, language, page, authors, publication_types, completed_date, revised_date, journal_title, journal_issn, journal_abb, journal_volume, journal_issue, journal_publish_info, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

    def main(self):
        pool = ThreadPool(10)
        pool.map(self.process_data, self.to_read_files)
        self.cursor.executemany(self.insert_query, self.values)
        self.cursor.close()
        self.conn.close()

    def check_text(self, input_tag):
        value = ''
        if input_tag is not None:
            value = input_tag.text
        return value

    def process_data(self, input_file):
        root = parse(path.join(self.to_read_path, input_file)).getroot()
        articles = root.findall('PubmedArticle')
        for article in articles:
            pubmed_id, doi = '', ''
            article_data = article.find('MedlineCitation')

            completed_year = self.check_text(article_data.find('DateCompleted/Year'))
            completed_month = self.check_text(article_data.find('DateCompleted/Month'))
            completed_day = self.check_text(article_data.find('DateCompleted/Day'))
            completed_date = '-'.join([completed_year, completed_month, completed_day]).strip('-')

            revised_year = self.check_text(article_data.find('DateRevised/Year'))
            revised_month = self.check_text(article_data.find('DateRevised/Month'))
            revised_day = self.check_text(article_data.find('DateRevised/Day'))
            revised_date = '-'.join([revised_year, revised_month, revised_day]).strip('-')

            journal_title = self.check_text(article_data.find('Article/Journal/Title'))
            journal_issn = self.check_text(article_data.find('Article/Journal/ISSN'))
            journal_abb = self.check_text(article_data.find('Article/Journal/ISOAbbreviation'))
            journal_volume = self.check_text(article_data.find('Article/Journal/JournalIssue/Volume'))
            journal_issue = self.check_text(article_data.find('Article/Journal/JournalIssue/Issue'))
            journal_pub_year = self.check_text(article_data.find('Article/Journal/JournalIssue/PubDate/Year'))
            journal_pub_month = self.check_text(article_data.find('Article/Journal/JournalIssue/PubDate/Month'))
            journal_publish_info = '-'.join([journal_pub_year, journal_pub_month]).strip('-')

            article_title = self.check_text(article_data.find('Article/ArticleTitle'))
            article_page = self.check_text(article_data.find('Article/Pagination/MedlinePgn'))
            article_language = self.check_text(article_data.find('Article/Language'))
            authors = '<>'.join([' '.join((self.check_text(item.find('ForeName')), self.check_text(item.find('LastName')))) for item in article_data.findall('Article/AuthorList/Author')])
            publication_types = '<>'.join([self.check_text(item) for item in article_data.findall('Article/PublicationTypeList/PublicationType')])

            pubmed_nodes = article.findall('PubmedData/ArticleIdList/ArticleId')
            for node in pubmed_nodes:
                if node.get('IdType', '').lower() == 'pubmed':
                    pubmed_id = node.text
                elif node.get('IdType', '').lower() == 'doi':
                    doi = node.text

            self.values.append((pubmed_id, doi, article_title, article_language, article_page, authors, publication_types, completed_date, revised_date, journal_title, journal_issn, journal_abb, journal_volume, journal_issue, journal_publish_info))

        move(path.join(self.to_read_path, input_file), self.to_move_path)

if __name__ == '__main__':
    PubmedXMLParser().main()
