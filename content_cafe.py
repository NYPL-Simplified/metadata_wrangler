from collections import Counter
import datetime
import requests
import logging
from nose.tools import set_trace
from bs4 import BeautifulSoup
from suds.client import Client as SudsClient

# Tone down the verbose Suds logging.
logging.getLogger('suds').setLevel(logging.ERROR)

from core.config import CannotLoadConfiguration
from core.coverage import (
    IdentifierCoverageProvider,
    CoverageFailure,
)
from core.model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Measurement,
    Identifier,
)
from core.util.summary import SummaryEvaluator

from mirror import (
    CoverImageMirror,
    ImageScaler,
)


class ContentCafeCoverageProvider(IdentifierCoverageProvider):
    SERVICE_NAME = "Content Cafe Coverage Provider"
    DEFAULT_BATCH_SIZE = 25
    INPUT_IDENTIFIER_TYPES = [Identifier.ISBN]
    DATA_SOURCE_NAME = DataSource.CONTENT_CAFE
    
    def __init__(self, _db, api=None, uploader=None, **kwargs):
        super(ContentCafeCoverageProvider, self).__init__(
            _db, preregistered_only=True, **kwargs
        )
        if api:
            self.content_cafe = api
            self.mirror = api.mirror
        else:
            self.mirror = ContentCafeCoverImageMirror(
                self._db, uploader=uploader
            )
            self.content_cafe = api or ContentCafeAPI.from_config(
                self._db, self.mirror, uploader=uploader
            )

    def process_item(self, identifier):
        try:
            self.content_cafe.mirror_resources(identifier)
            return identifier
        except Exception as e:
            self.log.error('Coverage error for %r', identifier, exc_info=e)
            return self.failure(identifier, repr(e), transient=True)


class ContentCafeCoverImageMirror(CoverImageMirror):
    """Downloads images from Content Cafe."""

    DATA_SOURCE = DataSource.CONTENT_CAFE


class ContentCafeAPI(object):
    """Associates up to four resources with an ISBN."""

    BASE_URL = "http://contentcafe2.btol.com/"
    ONE_YEAR_AGO = datetime.timedelta(days=365)

    image_url = BASE_URL + "ContentCafe/Jacket.aspx?userID=%(userid)s&password=%(password)s&Type=L&Value=%(isbn)s"
    overview_url= BASE_URL + "ContentCafeClient/ContentCafe.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    review_url = BASE_URL + "ContentCafeClient/ReviewsDetail.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    summary_url = BASE_URL + "ContentCafeClient/Summary.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    excerpt_url = BASE_URL + "ContentCafeClient/Excerpt.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    author_notes_url = BASE_URL + "ContentCafeClient/AuthorNotes.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"

    log = logging.getLogger("Content Cafe API")

    @classmethod
    def from_config(cls, _db, mirror, **kwargs):
        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.CONTENT_CAFE,
            ExternalIntegration.METADATA_GOAL
        )
        if not integration or not (integration.username and integration.password):
            raise CannotLoadConfiguration('Content Cafe not properly configured')

        return cls(
            _db, mirror, integration.username, integration.password,
            **kwargs
        )

    def __init__(self, _db, mirror, user_id, password, uploader=None,
                 soap_client=None):
        self._db = _db

        self.mirror = mirror
        if self.mirror:
            self.scaler = ImageScaler(_db, [self.mirror], uploader=uploader)
        else:
            self.scaler = None

        self.user_id = user_id
        self.password = password
        self.soap_client = (
            soap_client or ContentCafeSOAPClient(user_id, password)
        )

    @property
    def data_source(self):
        return DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

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
        self.measure_popularity(isbn_identifier, self.soap_client.ONE_YEAR_AGO)

    def get_associated_web_resources(
            self, identifier, args, url,
            phrase_indicating_missing_data,
            rel, scrape_method):
        url = url % args
        self.log.debug("Getting associated resources for %s", url)
        response = requests.get(url)
        content_type = response.headers['Content-Type']
        hyperlinks = []
        already_seen = set()
        if not phrase_indicating_missing_data in response.content:
            self.log.info("Found %s %s Content!", identifier.identifier, rel)
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
                    self.log.debug(
                        "Content: %s",
                        hyperlink.resource.representation.content[:75])
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

    def measure_popularity(self, identifier, cutoff=None):
        if identifier.type != Identifier.ISBN:
            raise Error("I can only measure the popularity of ISBNs.")
        value = self.soap_client.estimated_popularity(identifier.identifier)
        # Even a complete lack of popularity data is useful--it tells
        # us there's no need to check again anytime soon.
        measurement = identifier.add_measurement(
            self.data_source, Measurement.POPULARITY, value)

        # Since there is no associated Edition, now is a good time to
        # normalize the value.
        return measurement.normalized_value

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

class ContentCafeSOAPError(IOError):
    pass

class ContentCafeSOAPClient(object):

    WSDL_URL = "http://contentcafe2.btol.com/ContentCafe/ContentCafe.asmx?WSDL"

    DEMAND_HISTORY = "DemandHistoryDetail"

    ONE_YEAR_AGO = datetime.timedelta(days=365)

    def __init__(self, user_id, password, wsdl_url=None):
        wsdl_url = wsdl_url or self.WSDL_URL
        self.user_id=user_id
        self.password = password
        self.soap = SudsClient(wsdl_url)

    def get_content(self, key, content):
        data = self.soap.service.Single(
            userID=self.user_id, password=self.password,
            key=key, content=content)
        if hasattr(data, 'Error'):
            raise ContentCafeSOAPError(data.Error)
        else:
            return data

    def estimated_popularity(self, key, cutoff=None):
        data = self.get_content(key, self.DEMAND_HISTORY)
        gathered = self.gather_popularity(data)
        return self.estimate_popularity(gathered, cutoff)

    def gather_popularity(self, detail):
        by_year_and_month = Counter()
        [request_item] = detail.RequestItems.RequestItem
        items = request_item.DemandHistoryItems
        if items == '':
            # This ISBN is completely unknown.
            return None
        for history in items.DemandHistoryItem:
            key = datetime.date(history.Year, history.Month, 1)
            by_year_and_month[key] += int(history.Demand)
        return by_year_and_month

    def estimate_popularity(self, by_year_and_month, cutoff=None):
        """Turn demand data into a library-friendly estimate of popularity.

        :return: The book's maximum recent popularity, or one-half its
        maximum all-time popularity, whichever is greater. If there are no
        measurements, returns None. This is different from zero, which
        indicates a measured lack of demand.

        :param cutoff: The point at which "recent popularity" stops.
        """
        lifetime = []
        recent = []
        if by_year_and_month is None:
            return None
        if isinstance(cutoff, datetime.timedelta):
            cutoff = datetime.date.today() - cutoff
        for k, v in by_year_and_month.items():
            lifetime.append(v)
            if not cutoff or k >= cutoff:
                recent.append(v)
        if recent:
            return max(max(recent), max(lifetime) * 0.5)
        elif lifetime:
            return max(lifetime) * 0.5
        else:
            return None
