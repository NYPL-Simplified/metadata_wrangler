import os
import requests
from nose.tools import set_trace
from bs4 import BeautifulSoup

from sqlalchemy import and_
from core.coverage import CoverageProvider
from core.model import (
    DataSource,
    Hyperlink,
    Resource,
    Identifier,
)

from mirror import (
    CoverImageMirror,
    ImageScaler,
)
from core.util.summary import SummaryEvaluator

class ContentCafeCoverageProvider(CoverageProvider):
    def __init__(self, _db):
        self._db = _db
        self.input_source = DataSource.lookup(_db, DataSource.CONTENT_CAFE)
        self.output_source = self.input_source
        self.mirror = ContentCafeCoverImageMirror(self._db)
        self.content_cafe = ContentCafeAPI(self._db, self.mirror)

        super(ContentCafeCoverageProvider, self).__init__(
            "Content Cafe Coverage Provider",
            self.input_source, self.output_source)

    def process_edition(self, identifier):
        self.content_cafe.mirror_resources(identifier)
        return True

class ContentCafeCoverImageMirror(CoverImageMirror):
    """Downloads images from Content Cafe."""

    DATA_SOURCE = DataSource.CONTENT_CAFE

class ContentCafeAPI(object):
    """Associates up to four resources with an ISBN."""

    BASE_URL = "http://contentcafe2.btol.com/"

    image_url = BASE_URL + "ContentCafe/Jacket.aspx?userID=%(userid)s&password=%(password)s&Type=L&Value=%(isbn)s"
    overview_url= BASE_URL + "ContentCafeClient/ContentCafe.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    review_url = BASE_URL + "ContentCafeClient/ReviewsDetail.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    summary_url = BASE_URL + "ContentCafeClient/Summary.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    excerpt_url = BASE_URL + "ContentCafeClient/Excerpt.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    author_notes_url = BASE_URL + "ContentCafeClient/AuthorNotes.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"

    def __init__(self, db, mirror, user_id=None, password=None):
        self._db = db
        self.mirror = mirror
        self.scaler = ImageScaler(db, [self.mirror])
        self.data_source = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        self.user_id = user_id or os.environ['CONTENT_CAFE_USER_ID']
        self.password = password or os.environ['CONTENT_CAFE_PASSWORD']

    def mirror_resources(self, isbn_identifier):
        """Associate a number of resources with the given ISBN.
        """
        isbn = isbn_identifier.identifier

        args = dict(userid=self.user_id, password=self.password, isbn=isbn)
        image_url = self.image_url % args
        hyperlink, is_new = isbn_identifier.add_link(
            Hyperlink.IMAGE, image_url, self.data_source)
        representation = self.mirror.mirror_hyperlink(hyperlink)
        if representation.status_code == 404:
            # Content Cafe served us an HTML page instead of an
            # image. This indicates that Content Cafe has no knowledge
            # of this ISBN. There is no need to make any more
            # requests.
            return True

        self.mirror.uploader.mirror_one(representation)
        self.scaler.scale_edition(isbn_identifier)
        self.get_descriptions(isbn_identifier, args)
        self.get_excerpt(isbn_identifier, args)
        self.get_reviews(isbn_identifier, args)
        self.get_author_notes(isbn_identifier, args)

    def get_associated_web_resources(
            self, identifier, args, url, 
            phrase_indicating_missing_data,
            rel, scrape_method):
        url = url % args
        print url
        response = requests.get(url)
        content_type = response.headers['Content-Type']
        hyperlinks = []
        already_seen = set()
        if not phrase_indicating_missing_data in response.content:
            print " %s %s Content!" % (identifier.identifier, rel)
            soup = BeautifulSoup(response.content, "lxml")
            resource_contents = scrape_method(soup)
            if resource_contents:
                for content in resource_contents:
                    if content in already_seen:
                        continue
                    already_seen.add(content)
                    hyperlink, is_new = identifier.add_link(
                        rel, None, self.data_source, media_type="text/html", 
                        content=content)
                    hyperlinks.append(hyperlink)
                    print " ", hyperlink.resource.representation.content[:75]
            print
        return hyperlinks

    def get_reviews(self, identifier, args):
        return self.get_associated_web_resources(
            identifier, args, self.review_url,
            'No review info exists for this item',
            Hyperlink.REVIEW, self._scrape_list)

    def get_descriptions(self, identifier, args):
        hyperlinks = list(self.get_associated_web_resources(
            identifier, args, self.summary_url,
            'No annotation info exists for this item',
            Hyperlink.DESCRIPTION, self._scrape_list))
        if not hyperlinks:
            return hyperlinks

        # Since we get multiple descriptions, and there is no
        # associated Edition, now is a good time to evaluate the quality
        # of the descriptions. This will make it easy to pick the best one
        # when this identifier is looked up.
        evaluator = SummaryEvaluator(bad_phrases=[])
        by_content = dict()
        for link in hyperlinks:
            content = link.resource.representation.content
            evaluator.add(content)
        evaluator.ready()
        for link in hyperlinks:
            resource = link.resource
            content = resource.representation.content
            quality = evaluator.score(content)
            resource.set_estimated_quality(quality)
            resource.update_quality()
        return hyperlinks

    def get_author_notes(self, identifier, args):
        return self.get_associated_web_resources(
            identifier, args, self.author_notes_url,
            'No author notes info exists for this item',
            Hyperlink.AUTHOR, self._scrape_one)

    def get_excerpt(self, identifier, args):
        return self.get_associated_web_resources(
            identifier, args, self.excerpt_url,
            'No excerpt info exists for this item', Hyperlink.SAMPLE,
            self._scrape_one)

    @classmethod
    def _scrape_list(cls, soup):
        table = soup.find('table', id='Table_Main')
        if table:
            for header in table.find_all('td', class_='SectionHeader'):
                content = header.parent.next_sibling
                if content.name != 'tr':
                    continue
                if not content.td:
                    continue
                yield content.td.encode_contents()

    @classmethod
    def _scrape_one(cls, soup):
        table = soup.find('table', id='Table_Main')
        if not table:
            return []
        if table.tr and table.tr.td:
            return [table.tr.td.encode_contents()]
        else:
            return []
