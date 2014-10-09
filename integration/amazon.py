import isbnlib
import random
import requests
import time
import os
import re
from cStringIO import StringIO
from lxml import etree 
from integration import (
    FilesystemCache,
    MultipageFilesystemCache,
    XMLParser,
)
from model import (
    CoverageProvider,
    DataSource,
    Identifier,
    Measurement,
    Subject,
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
        time.sleep(1 + random.random())
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise IOError(response.status_code)
        if not response.text:
            raise IOError("Empty response")
        return response

    def __init__(self, data_directory):
        path = os.path.join(data_directory, DataSource.AMAZON)
        bibliographic_cache = os.path.join(path, "bibliographic")
        if not os.path.exists(bibliographic_cache):
            os.makedirs(bibliographic_cache)
        self.bibliographic_cache = FilesystemCache(
            bibliographic_cache, subdir_chars=4, substring_from_beginning=False,
            compress=True)
        review_cache = os.path.join(path, "review")        
        if not os.path.exists(bibliographic_cache):
            os.makedirs(bibliographic_cache)
        self.review_cache = MultipageFilesystemCache(
            review_cache, subdir_chars=4, substring_from_beginning=False,
            compress=True)

    def scrape(self, asin):
        identifiers, subjects, rating = self.scrape_bibliographic_info(asin)
        reviews = self.scrape_reviews(asin)
        return identifiers, subjects, rating, reviews
    
    def get_bibliographic_info(self, asin):
        if self.bibliographic_cache.exists(asin):
            return self.bibliographic_cache.open(asin).read()

        url = self.BIBLIOGRAPHIC_URL % dict(asin=asin)
        response = self.get(url)
        self.bibliographic_cache.store(asin, response.text.encode("utf8"))
        return response.text

    def get_reviews(self, asin, page, force=False):
        if not force and self.review_cache.exists(asin, page):
            return self.review_cache.open(asin, page).read()

        url = self.REVIEW_URL % dict(
            asin=asin, page_number=page, 
            sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        if page > 1:
            old_url = self.REVIEW_URL % dict(
            asin=asin, page_number=page-1, 
            sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        else:
            old_url = self.BIBLIOGRAPHIC_URL % dict(asin=asin)
        print url
        response = self.get(url, old_url)
        self.review_cache.store(asin, page, response.text)
        return response.text

    def scrape_bibliographic_info(self, asin):
        print "ASIN %s" % asin
        parser = AmazonBibliographicParser()
        data = self.get_bibliographic_info(asin)
        return parser.process_all(data)

    def scrape_reviews(self, asin):
        parser = AmazonReviewParser()
        for page in range(1,11):
            reviews_on_this_page = 0
            try:
                reviews = self.get_reviews(asin, page)
            except IOError, e:
                return
            for page_reviews in parser.process_all(reviews):
                for review in page_reviews:
                    yield review
                    reviews_on_this_page += 1
            if reviews_on_this_page == 0 or reviews_on_this_page < 10:
                break


class AmazonBibliographicParser(XMLParser):

    IDENTIFIER_IN_URL = re.compile("/dp/([^/]+)/")
    BLACKLIST_FORMAT_SUBSTRINGS = ['Large Print', 'Audio']
    NAMESPACES = {}
    QUALITY_RE = re.compile("([0-9.]+) out of")
    PAGE_COUNT_RE = re.compile("([0-9]+) page")
    SALESRANK_RES = [re.compile("#([0-9,]+) Paid in"),
                     re.compile("#([0-9,]+) Free in"),
                     re.compile("#([0-9,]+) in")]

    KEYWORD_BLACKLIST = set(["books"])
    PARTIAL_KEYWORD_BLACKLIST = set(["kindle", "ebook", "amazon"])

    NUMBERS = re.compile("[0-9-]+")

    def add_keywords(self, container, keywords, exclude_set):
        for kw in keywords:
            l = kw.lower()
            if not l:
                continue
            if self.NUMBERS.match(l):
                continue
            if (l in self.KEYWORD_BLACKLIST
                or l in exclude_set
                or isbnlib.is_isbn10(l)):
                continue
            ok = True
            for partial in self.PARTIAL_KEYWORD_BLACKLIST:
                if partial in l:
                    ok = False
                    break
            if not ok:
                continue
            container.add(kw)

    def process_all(self, string):
        parser = etree.HTMLParser()
        if isinstance(string, unicode):
            string = string.encode("utf8")
        root = etree.parse(StringIO(string), parser)

        identifiers = []
        measurements = {}
        keywords = set([])
        bib = dict(identifiers=identifiers, measurements=measurements,
                   keywords=keywords)
        exclude_tags = set()

        # Find the title, mainly so we can exclude it if it shows up
        # in keywords.
        title = None
        for title_id in ('productTitle', 'btAsinTitle'):
            title_tag = self._xpath1(root, '//*[@id="%s"]' % title_id)
            if title_tag is not None:
                title = title_tag.text.strip()
                break
        if title:
            bib['title'] = title
            exclude_tags.add(title.lower())

        # Find other editions of this book.
        edition_tags = root.xpath('//a[@class="title-text"]')
        for edition_tag in edition_tags:
            format = "".join([x.strip() for x in edition_tag.xpath("span/text()")])
            usable_format = True
            for i in self.BLACKLIST_FORMAT_SUBSTRINGS:
                if i in format:
                    usable_format = False
                    break
            if not usable_format:
                continue

            if not 'Kindle' in format:
                print "Unknown format: %s" % format
            href = edition_tag.attrib['href']
            m = self.IDENTIFIER_IN_URL.search(href)
            if not m:
                print "Could not find identifier in %s" % href
                continue

            identifier = m.groups()[0]
            identifier_type = Identifier.ASIN
            if isbnlib.is_isbn10(identifier) or isbnlib.is_isbn13(identifier):
                identifier_type = Identifier.ISBN

            identifiers.append((identifier_type, identifier))
            # Exclude identifiers if they also show up in tags.
            exclude_tags.add(identifier)

        # Try two different techniques to find classifications.
        # First, look in a <meta> tag for keywords.
        keyword_tag = self._xpath1(root, '//meta[@name="keywords"]')
        if keyword_tag is not None:
            tag_keywords = keyword_tag.attrib['content'].split(",")
            self.add_keywords(keywords, tag_keywords, exclude_tags)

        # Then look for categorizations.
        similar_tag = self._xpath1(
            root, '//*[text()="Look for Similar Items by Category"]')
        if similar_tag is not None:
            category_keywords = set([])
            for item in similar_tag.xpath("..//ul/li"):
                links = item.xpath("a")
                for l in links:
                    category_keywords.add(l.text.strip())
            self.add_keywords(keywords, category_keywords, exclude_tags)

        measurements[Measurement.RATING] = self.get_quality(root)
        measurements[Measurement.POPULARITY] = self.get_popularity(root)
        page_count = self.get_page_count(root)
        if page_count:
            measurements[Measurement.PAGE_COUNT] = page_count 
        return bib

    def _cls(self, tag_name, class_name):
        return '//%s[contains(concat(" ", normalize-space(@class), " "), " %s ")]' % (tag_name, class_name)

    def get_quality(self, root):
        # Look in three different places for a star rating.
        for xpath in (
                '//*[@id="acrReviewStars"]',
                self._cls("div", "acrStars") + "/span",
                '//*[@id="acrPopover"]'):
            match = self._xpath1(root, xpath)
            if match is not None and 'title' in match.attrib:
                quality = match.attrib['title']
                break
        if not quality:
            return None
        m = self.QUALITY_RE.search(quality)
        if m:
            quality = float(m.groups()[0])
        else:
            quality = None
        return quality

    def get_popularity(self, root):
        # Try a number of ways to measure the sales rank.
        sales_rank_text = self._xpath1(
            root, '//*[@id="SalesRank"]/b/following-sibling::text()').strip()
        popularity = None
        for r in self.SALESRANK_RES:
            m = r.search(sales_rank_text)
            if m:
                popularity = int(m.groups()[0].replace(",", ""))
                break
        return popularity

    def get_page_count(self, root):
        """Measure the page count, if it's available."""
        page_count_text = self._xpath1(
            root, '//*[@id="pageCountAvailable"]/span/text()')
        if not page_count_text:
            return None
        m = self.PAGE_COUNT_RE.search(page_count_text)
        if not m:
            return None
        return int(m.groups()[0])

class AmazonReviewParser(XMLParser):

    NAMESPACES = {}

    def process_all(self, string):
        parser = etree.HTMLParser()
        if isinstance(string, unicode):
            string = string.encode("utf8")
        for review in super(AmazonReviewParser, self).process_all(
                string, "//*[@id='productReviews']",
            parser=parser):
            yield review

    def process_one(self, reviewset, ns):
        text = []
        for review in reviewset.xpath("//div[@class='reviewText']",
                                      namespaces=ns):
            b = self._xpath1(review, "../div/span/b")
            if b is None:
                title = None
            else:
                title = b.text.strip()
            review_text = review.xpath("text()")
            yield title, "\n\n".join(review_text)


class AmazonCoverageProvider(CoverageProvider):
    
    SERVICE_NAME = "Amazon Coverage Provider"

    def __init__(self, db, data_directory, identifier_types=None):
        self.amazon = AmazonScraper(data_directory)
        self.db = db
        if not identifier_types:
            identifier_types = [Identifier.ISBN, Identifier.ASIN]
        self.coverage_source = DataSource.lookup(db, DataSource.AMAZON)

        super(AmazonCoverageProvider, self).__init__(
            self.SERVICE_NAME,
            identifier_types,
            self.coverage_source,
            workset_size=50)
       
    @property
    def editions_that_need_coverage(self):
        """Returns identifiers (not editions) that need coverage."""
        return Identifier.missing_coverage_from(
            self.db, self.input_sources, self.coverage_source)

    def process_edition(self, identifier):
        """Process an identifier (not an edition)."""
        i = identifier.identifier
        bibliographic = self.amazon.scrape_bibliographic_info(i)
        reviews = self.amazon.scrape_reviews(i)

        for type, other_identifier_id in bibliographic['identifiers']:
            other_identifier = Identifier.for_foreign_id(
                self._db, type, other_identifier_id)[0]
            identifier.equivalent_to(self.coverage_source, other_identifier, 1)

        for quantity, measurement in bibliographic['measurements'].items():
            if isinstance(measurement, tuple):
                measurement, weight = measurement
            else:
                weight = 1
            identifier.add_measurement(
                self.coverage_source, quantity, measurement, weight)
        
        for keyword in bibliographic['keywords']:
            identifier.classify(
                self.coverage_source, Subject.TAG, keyword)
