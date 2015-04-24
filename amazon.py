from nose.tools import set_trace
from datetime import timedelta
import isbnlib
import random
import requests
import time
import os
import re
import urlparse
import sys
from bs4 import BeautifulSoup
from cStringIO import StringIO
from lxml import etree 
from core.util.xmlparser import (
    XMLParser,
)
from core.coverage import CoverageProvider
from core.model import (
    DataSource,
    Equivalency,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from sqlalchemy import alias

class AmazonAPI(object):
    
    SORT_REVIEWS_BY_DATE = "bySubmissionDateDescending"
    SORT_REVIEWS_BY_HELPFULNESS = "byRankDescending"

    BIBLIOGRAPHIC_URL = 'http://www.amazon.com/exec/obidos/ASIN/%(asin)s'
    REVIEW_URL = 'http://www.amazon.com/product-reviews/%(asin)s/ref=cm_cr_dp_see_all_btm?ie=UTF8&showViewpoints=1&pageNumber=%(page_number)s&sortBy=%(sort_by)s'

    MAX_BIBLIOGRAPHIC_AGE = timedelta(days=30*3)
    MAX_REVIEW_AGE = timedelta(days=30*6)

    RATE_LIMIT_TEXT = "Sorry, we just need to make sure you're not a robot. For best results, please make sure your browser is accepting cookies."

    def __init__(self, _db):
        self._db = _db
        self.data_source = DataSource.lookup(_db, DataSource.AMAZON)

    def fetch(self, identifier):
        identifiers, subjects, rating = self.fetch_bibliographic_info(
            identifier)
        reviews = self.fetch_reviews(identifier)
        reviews = []
        return identifiers, subjects, rating, reviews
    
    def get_bibliographic_info(self, identifier, get_method=None):
        if get_method:
            pause = 0
        else:
            get_method = Representation.browser_http_get
            pause = (1 + random.random()) * 4
        asin = identifier.identifier
        if isbnlib.is_isbn13(asin):
            asin = isbnlib.to_isbn10(asin)
        url = self.BIBLIOGRAPHIC_URL % dict(asin=asin)
        representation, cached = Representation.get(
            self._db, url, get_method,
            pause_before=pause,
            max_age=self.MAX_BIBLIOGRAPHIC_AGE)
        if self.RATE_LIMIT_TEXT in representation.content and cached:
            # Force a refresh.
            representation, cached = Representation.get(
                self._db, url, get_method,
                pause_before=pause,
                max_age=0)
        if self.RATE_LIMIT_TEXT in representation.content:
            if sys.stdin.isatty():
                # We're being run in a terminal. A human being can fix this.
                representation = AmazonRateLimitCAPTCHAClient(self._db, url, captcha_content=representation.content).process()
            else:
                raise Exception("Rate limit triggered on %s" % url)

        return representation, cached

    def get_reviews(self, identifier, page, force=False, get_method=None):
        if get_method:
            pause = 0
        else:
            get_method = Representation.browser_http_get
            pause = (1 + random.random()) * 4
        get_method = get_method or Representation.browser_http_get

        if force:
            max_age=timedelta(seconds=0)
        else:
            max_age=self.MAX_REVIEW_AGE

        asin = identifier.identifier
        if isbnlib.is_isbn13(asin):
            asin = isbnlib.to_isbn10(asin)

        url = self.REVIEW_URL % dict(
            asin=asin, page_number=page, 
            sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        extra_request_headers = dict()
        if page > 1:
            referrer = self.REVIEW_URL % dict(
                asin=identifier.identifier, page_number=page-1, 
                sort_by=self.SORT_REVIEWS_BY_HELPFULNESS)
        else:
            referrer = self.BIBLIOGRAPHIC_URL % dict(asin=identifier.identifier)
        extra_request_headers['Referer'] = referrer

        representation, cached = Representation.get(
            self._db, url, get_method,
            extra_request_headers=extra_request_headers,
            max_age=max_age, pause_before=pause)

        if self.RATE_LIMIT_TEXT in representation.content and cached:
            # Force a refresh.
            representation, cached = Representation.get(
                self._db, url, get_method,
                pause_before=pause,
                max_age=0)
        if self.RATE_LIMIT_TEXT in representation.content:
            if sys.stdin.isatty():
                # We're being run in a terminal. A human being can fix this.
                representation = AmazonRateLimitCAPTCHAClient(self._db, url, captcha_content=representation.content).process()
            else:
                raise Exception("Rate limit triggered on %s" % url)

        if representation.status_code == 404:
            print "Amazon has no knowledge of ASIN %s" % asin
        elif not cached and not representation.content:
            print "No content!"
            # Sleep to deal with possible rate limiting.
            time.sleep(60)
        return representation

    def fetch_bibliographic_info(self, identifier):
        parser = AmazonBibliographicParser()
        representation, ignore = self.get_bibliographic_info(identifier)
        if representation.has_content:
            return parser.process_all(representation.content)
        return None

    def fetch_reviews(self, identifier, force=False):
        # TODO: Currently we don't use reviews enough to justify the
        # large time expense fetching them. For the time being, act as
        # though there are no reviews.
        if not force:
            return []

        parser = AmazonReviewParser()
        all_reviews = []
        for page in range(1,11):
            reviews_on_this_page = 0
            representation = self.get_reviews(identifier, page)
            if not representation.has_content:
                break
            for page_reviews in parser.process_all(representation.content):
                for review in page_reviews:
                    all_reviews.append(review)
                    reviews_on_this_page += 1
            if reviews_on_this_page == 0 or reviews_on_this_page < 10:
                # print "Only %s reviews on the page." %  reviews_on_this_page
                break
            # print "%d reviews so far" % len(all_reviews)
        return all_reviews

class AmazonParser(XMLParser):
    pass

class AmazonBibliographicParser(AmazonParser):

    IDENTIFIER_IN_URL = re.compile("/dp/([^/]+)/")
    BLACKLIST_FORMAT_SUBSTRINGS = ['Large Print', 'Audio', 'Audible',
                                   'Multimedia', 'CD', 'DVD']
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
            container.add(kw.strip())

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

            if not 'Kindle' in format and not 'Hardcover' in format and not 'Paperback' in format:
                pass
                #print "Unknown format: %s" % format
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

        quality = self.get_quality(root)
        if quality:
            measurements[Measurement.RATING] = quality

        popularity = self.get_popularity(root)
        if popularity:
            measurements[Measurement.POPULARITY] = popularity
        page_count = self.get_page_count(root)
        if page_count:
            measurements[Measurement.PAGE_COUNT] = page_count 

        bib[Subject.AGE_RANGE] = self.get_age_range(root)        
        bib[Subject.GRADE_LEVEL] = self.get_grade_level(root)
        bib[Subject.LEXILE_SCORE] = self.get_lexile_score(root)

        return bib

    def get_quality(self, root):
        # Look in three different places for a star rating.
        quality = None
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
            root, '//*[@id="SalesRank"]/b/following-sibling::text()')
        if not sales_rank_text:
            return None
        sales_rank_text = sales_rank_text.strip()
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

    def _text_after_b_tag_with_contents(self, root, contents):
        xpath = ("//b[text()[contains(.,'%s')]]/following-sibling::text()" 
                 % contents)
        contents = self._xpath1(root, xpath)
        if contents:
            return contents.strip()
        return None

    def _text_after_span_with_contents(self, root, contents):
        xpath = ('//span[@class="byLinePipe"][text()[contains(.,"%s")]]/following-sibling::span/text()'
                 % contents)
        contents = self._xpath1(root, xpath)
        if contents:
            return contents.strip()
        return None

    def get_age_range(self, root):
        """Measure the age range, if it's available."""
        return (self._text_after_b_tag_with_contents(root, 'Age Range:')
                or self._text_after_span_with_contents(root, "Age Level:"))

    def get_grade_level(self, root):
        """Measure the grade level, if it's available."""
        return (self._text_after_b_tag_with_contents(root, 'Grade Level:')
                or self._text_after_span_with_contents(root, "Grade Level:"))

    def get_lexile_score(self, root):
        """Measure the Lexile score, if it's available."""
        return self._text_after_b_tag_with_contents(root, 'Lexile Measure:')

class AmazonReviewParser(AmazonParser):

    NAMESPACES = {}

    def process_all(self, string):
        parser = etree.HTMLParser()
        if isinstance(string, unicode):
            string = string.encode("utf8")

        for review in super(AmazonReviewParser, self).process_all(
                string, "//html",
            parser=parser):
            yield review

    def process_one(self, reviewset, ns):
        text = []
        # There are two different web pages we might get.
        reviews = reviewset.xpath(self._cls("div", "reviewText"))
        for review in reviews:
            b = self._xpath1(review, "../div/span/b")
            if b is None:
                title = None
            else:
                title = b.text
                if title:
                    title = title.strip()
            review_text = review.xpath("text()")
            yield title, "\n\n".join(review_text)

        reviews = reviewset.xpath(self._cls("div", "review"))
        for review in reviews:
            title = self._xpath1(review, self._cls("a", "review-title")).text
            review_tag = None
            review_text = []
            for tag_name in ('div', 'span'):
                review_tag = self._xpath1(
                    review, self._cls(tag_name, "review-text"))
                if review_tag is not None:
                    review_text = review_tag.xpath("text()")
                    break
            yield title, "\n\n".join(review_text)

class AmazonCoverageProvider(CoverageProvider):
    
    SERVICE_NAME = "Amazon Coverage Provider"

    def __init__(self, db, identifier_types=None):
        self.amazon = AmazonAPI(db)
        self.db = db
        if not identifier_types:
            identifier_types = [Identifier.ISBN, Identifier.ASIN]
        self.coverage_source = DataSource.lookup(db, DataSource.AMAZON)

        super(AmazonCoverageProvider, self).__init__(
            self.SERVICE_NAME,
            identifier_types,
            self.coverage_source,
            workset_size=10)
       
    @property
    def editions_that_need_coverage(self):
        """Returns identifiers (not editions) that need coverage."""
        q = Identifier.missing_coverage_from(
            self.db, self.input_sources, self.coverage_source)
        return q

    def process_edition(self, identifier):
        """Process an identifier (not an edition)."""
        bibliographic = self.amazon.fetch_bibliographic_info(identifier)

        if not bibliographic:
            return True

        reviews = self.amazon.fetch_reviews(identifier)
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
        
        # These classifications are just okay.
        for keyword in bibliographic['keywords']:
            identifier.classify(
                self.coverage_source, Subject.TAG, keyword)

        # These classifications are highly trustworthy.
        for classification in [
                Subject.LEXILE_SCORE, Subject.GRADE_LEVEL, Subject.AGE_RANGE]:
            value = bibliographic.get(classification)
            if value:
                identifier.classify(
                    self.coverage_source, classification, value, weight=100)

        return True


class AmazonRateLimitCAPTCHAClient(object):

    def __init__(self, _db, captcha_url, captcha_content=None):

        self._db = _db
        self.captcha_url = captcha_url
        if not captcha_content:
            print "So the problematic URL is %s..." % captcha_url
            rep, cached = Representation.get(_db, captcha_url, max_age=0)
            if AmazonAPI.RATE_LIMIT_TEXT not in rep.content:
                print "Looks fine to me:"
                print rep.content
                captcha_content = None
            else:
                self.captcha_content = rep.content
        self.captcha_content = captcha_content

    def process(self):
        if not self.captcha_content:
            return None

        soup = BeautifulSoup(self.captcha_content)
        form = soup.find('form', action='/errors/validateCaptcha')
        fields = {}
        for i in form.find_all('input', type='hidden'):
            fields[i['name']] = i['value']

        captcha_field_name = form.find('input', type='text')['name']

        print "CAPTCHA URL is:"
        for img in form.find_all('img'):
            print img['src']
        print "Enter CAPTCHA value from URL:"
        value = sys.stdin.readline().strip()
        fields[captcha_field_name] = value

        field_data = [k + "=" + v for k, v in fields.items()]
        url = urlparse.urljoin("http://www.amazon.com", form['action']) + "?" + "&".join(field_data)
        print "Okay, trying %s" % url
        referer = dict(Referer=self.captcha_url)
        rep, cached = Representation.get(
            self._db, url, extra_request_headers=referer, 
            max_age=0)
        return rep
