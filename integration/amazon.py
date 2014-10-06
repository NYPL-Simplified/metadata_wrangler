import isbnlib
import requests
import os
from lxml import etree 
from integration import (
    FilesystemCache,
    MultipageFilesystemCache,
    XMLParser,
)
from model import (
    DataSource,
)
from pdb import set_trace

class AmazonScraper(object):
    
    SORT_REVIEWS_BY_DATE = "bySubmissionDateDescending"
    SORT_REVIEWS_BY_HELPFULNESS = "byRankDescending"

    BIBLIOGRAPHIC_URL = 'http://www.amazon.com/exec/obidos/ASIN/%(asin)s'
    REVIEW_URL = 'http://www.amazon.com/product-reviews/%(asin)s/ref=cm_cr_dp_see_all_btm?ie=UTF8&showViewpoints=1&pageNumber=%(page_number)s&sortBy=%(sort_by)s'

    USER_AGENT = "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.103 Safari/537.36"

    def get(self, url, referrer=None):
        headers = {"User-Agent" : self.USER_AGENT}
        if referrer:
            headers['Referer'] = referrer
        return requests.get(url, headers=headers)

    def __init__(self, data_directory):
        path = os.path.join(data_directory, DataSource.AMAZON)
        bibliographic_cache = os.path.join(path, "bibliographic")
        if not os.path.exists(bibliographic_cache):
            os.makedirs(bibliographic_cache)
        self.bibliographic_cache = FilesystemCache(path, subdir_chars=4,
                                                   take_subdir_from_start=False)
        review_cache = os.path.join(path, "review")        
        if not os.path.exists(bibliographic_cache):
            os.makedirs(bibliographic_cache)
        self.review_cache = MultipageFilesystemCache(
            review_cache, subdir_chars=4, take_subdir_from_start=False)

    def scrape(self, asin):
        identifiers, subjects, rating = self.scrape_bibliographic_info(asin)
        reviews = self.scrape_reviews(asin)
        return identifiers, subjects, rating, reviews
    
    def get_bibliographic_info(self, asin):
        if self.bibliographic_cache.exists(asin):
            return self.bibliographic_cache.open(asin).read()

        url = self.BIBLIOGRAPHIC_URL % dict(asin=asin)
        response = self.get(url)
        self.bibliographic_cache.store(asin, response.text)
        return response.text

    def get_reviews(self, asin, page):
        if self.review_cache.exists(asin, page):
            return self.review_cache.open(asin, page).read()

        url = self.REVIEW_URL % dict(
            asin=asin, page_number=page, 
            sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        if page > 1:
            old_url = self.REVIEW_URL % dict(
            asin=asin, page_number=page-1, 
            sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        else:
            old_url = None
        response = self.get(url, old_url)
        self.review_cache.store(asin, page, response.text)
        return response.text

    def scrape_bibliographic_info(self, asin):
        data = self.get_bibliographic_info(asin)
        

    def scrape_reviews(self, asin):
        parser = AmazonReviewParser()
        for page in range(1,3):
            reviews = self.get_reviews(asin, page)
            for review in parser.process_all(reviews):
                yield review
        


class AmazonReviewParser(XMLParser):

    def process_all(self, string):
        parser = etree.HTMLParser()
        return super(AmazonReviewParser, self).process_all(
                string, "//*[@id='productReviews']",
            parser=parser)

    def process_one(self, reviewset, ns):
        text = []
        for review in reviewset.xpath("//div[@class='reviewText']",
                                      namespace=ns):
            text.append(review.xpath("text()"))
        return "\n".join(text)

for i in AmazonScraper("/home/leonardr/data/").scrape_reviews("031624662X"):
    print i
