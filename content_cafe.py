from collections import Counter
import datetime
import os
import requests
import logging
from nose.tools import set_trace
from bs4 import BeautifulSoup
from suds.client import Client as SudsClient

# Tone down the verbose Suds logging.
logging.getLogger('suds').setLevel(logging.ERROR)

from sqlalchemy.orm.session import Session

from core.config import CannotLoadConfiguration
from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)
from core.metadata_layer import (
    LinkData,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
)
from core.model import (
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Measurement,
    Identifier,
)
from core.util.http import HTTP
from core.util.summary import SummaryEvaluator

from coverage_utils import MetadataWranglerBibliographicCoverageProvider

def load_file(filename):
    """Load a file from the Content Cafe subdirectory of files/."""
    this_dir = os.path.split(__file__)[0]
    content_dir = os.path.join(this_dir, "files", "content-cafe")
    path = os.path.join(content_dir, filename)
    return open(path).read()


class ContentCafeCoverageProvider(MetadataWranglerBibliographicCoverageProvider):
    """Create bare-bones Editions for ISBN-type Identifiers.

    An Edition will have no bibliographic information, apart from a
    possible title, but the Identifier should get some very
    important Hyperlinks associated, such as a cover image and
    description.
    """
    SERVICE_NAME = "Content Cafe Coverage Provider"
    INPUT_IDENTIFIER_TYPES = [Identifier.ISBN]
    DATA_SOURCE_NAME = DataSource.CONTENT_CAFE

    def __init__(self, collection, api=None, **kwargs):
        """Constructor.

        :param collection: A Collection.
        :param api: A ContentCafeAPI.
        :param replacement_policy: A ReplacementPolicy.
        :param kwargs: Any extra arguments to be passed into the
            BibliographicCoverageProvider superconstructor.
        """
        # Any ISBN-type identifier cataloged in a Collection needs to
        # be processed, whether or not it was explicitly registered.
        super(ContentCafeCoverageProvider, self).__init__(
            collection=collection, **kwargs
        )
        _db = Session.object_session(collection)
        self.content_cafe = api or ContentCafeAPI.from_config(self._db)

    def process_item(self, identifier):
        """Associate bibliographic metadata with the given Identifier.

        :param Identifier: Look up this Identifier on Content Cafe.
        """
        try:
            # Create a Metadata object.
            metadata = self.content_cafe.create_metadata(identifier)
            if not metadata:
                # TODO: The only time this is really a transient error
                # is when the book is too new for Content Cafe to know
                # about it, which isn't often. It would be best to
                # keep this as a transient failure but give it a relatively
                # long and exponentially increasing retry time.
                return self.failure(
                    identifier,
                    "Content Cafe has no knowledge of this identifier.",
                    transient=True
                )
            edition, is_new = Edition.for_foreign_id(
                self._db, self.data_source, identifier.type,
                identifier.identifier
            )
            # We're passing in collection=None even though we
            # technically have a Collection available, because our
            # goal is to add metadata for the book without reference
            # to any particular collection.
            metadata.apply(
                edition, collection=None, replace=self.replacement_policy
            )
            return identifier
        except Exception as e:
            self.log.error('Coverage error for %r', identifier, exc_info=e)
            return self.failure(identifier, repr(e), transient=True)


class ContentCafeAPI(object):
    """Gets data from Content Cafe to be associated with an ISBN."""

    BASE_URL = "http://contentcafe2.btol.com/"
    ONE_YEAR_AGO = datetime.timedelta(days=365)

    image_url = BASE_URL + "ContentCafe/Jacket.aspx?userID=%(userid)s&password=%(password)s&Type=L&Value=%(isbn)s"
    review_url = BASE_URL + "ContentCafeClient/ReviewsDetail.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    summary_url = BASE_URL + "ContentCafeClient/Summary.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    excerpt_url = BASE_URL + "ContentCafeClient/Excerpt.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"
    author_notes_url = BASE_URL + "ContentCafeClient/AuthorNotes.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"

    # This URL is not used -- it just links to the other URLs.
    overview_url= BASE_URL + "ContentCafeClient/ContentCafe.aspx?UserID=%(userid)s&Password=%(password)s&ItemKey=%(isbn)s"

    # An image file that starts with this bytestring is a placeholder
    # and should not be treated as a real book cover.
    STAND_IN_IMAGE_PREFIX = load_file("stand-in-prefix.png")

    # These pieces of text show up where a title normally would, but
    # they are Content Cafe status messages, not real book titles.
    KNOWN_BAD_TITLES = set([
        'No content currently exists for this item',
    ])

    log = logging.getLogger("Content Cafe API")

    @classmethod
    def from_config(cls, _db, **kwargs):
        """Create a ContentCafeAPI object based on database
        configuration.
        """
        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.CONTENT_CAFE,
            ExternalIntegration.METADATA_GOAL
        )
        if not integration or not (integration.username and integration.password):
            raise CannotLoadConfiguration('Content Cafe not properly configured')

        return cls(
            _db, integration.username, integration.password,
            **kwargs
        )

    def __init__(self, _db, user_id, password, soap_client=None, do_get=None):
        """Constructor.
        """
        self._db = _db
        self.user_id = user_id
        self.password = password
        self.soap_client = (
            soap_client or ContentCafeSOAPClient(user_id, password)
        )
        self.do_get = do_get or HTTP.get_with_timeout

    @property
    def data_source(self):
        return DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

    def create_metadata(self, isbn_identifier):
        """Make a Metadata object for the given Identifier.

        The Metadata object may include a cover image, descriptions,
        reviews, an excerpt, author notes, and a popularity measurement.

        :return: A Metadata object, or None if Content Cafe has no
        knowledge of this ISBN.
        """
        isbn = isbn_identifier.identifier

        args = dict(userid=self.user_id, password=self.password, isbn=isbn)
        image_url = self.image_url % args
        response = self.do_get(image_url)
        if response.status_code == 404:
            # Content Cafe served us an HTML page instead of an
            # image. This indicates that Content Cafe has no knowledge
            # of this ISBN -- if it knew _anything_ it would have a
            # cover image. There is no need to build a Metadata object.
            return None

        media_type = response.headers.get('Content-Type', 'image/jpeg')

        # Start building a Metadata object.
        metadata = Metadata(
            self.data_source, primary_identifier=isbn_identifier
        )
        
        # Add the cover image to it
        image = response.content
        if self.is_suitable_image(image):
            metadata.links.append(
                LinkData(
                    rel=Hyperlink.IMAGE, href=image_url, media_type=media_type,
                    content=response.content
                )
            )

        for annotator in (
            self.add_descriptions, self.add_excerpt,
            self.add_reviews, self.add_author_notes
        ):
            annotator(metadata, isbn_identifier, args)

        popularity = self.measure_popularity(
            isbn_identifier, self.ONE_YEAR_AGO
        )
        if popularity:
            metadata.measurements.append(popularity)
        return metadata

    def annotate_with_web_resources(
            self, metadata, identifier, args, url_template,
            phrase_indicating_missing_data,
            rel, scrape_method
    ):
        """Retrieve a URL, scrape information from the response,
        and use it to improve a Metadata object.

        Generally, this just means adding LinkData objects to .links,
        but in some cases we can also set the title.

        :param metadata: Anything found will be used to improve
           this Metadata object.
        """
        url = url_template % args
        self.log.debug("Getting associated resources for %s", url)
        response = self.do_get(url)
        content_type = response.headers['Content-Type']
        if (phrase_indicating_missing_data and 
            phrase_indicating_missing_data in response.content):
            # There is no data; do nothing.
            self.log.debug("No data is present.")
            return

        links = []
        already_seen = set()
        self.log.info("Found %s %s Content!", identifier.identifier, rel)
        soup = BeautifulSoup(response.content, "lxml")
        if not metadata.title:
            metadata.title = self._extract_title(soup)
        resource_contents = scrape_method(soup)
        if not resource_contents:
            self.log.debug("Data is present but contains no resources.")
            return

        for content in resource_contents:
            if not content:
                continue
            content = content.strip()
            if not content:
                continue
            if content in already_seen:
                continue
            already_seen.add(content)
            link = LinkData(
                rel=rel, href=None, media_type="text/html",
                content=content
            )
            metadata.links.append(link)
            self.log.debug("Content: %s", content[:75])

    def add_reviews(self, metadata, identifier, args):
        return self.annotate_with_web_resources(
            metadata, identifier, args, self.review_url,
            'No review info exists for this item',
            Hyperlink.REVIEW, self._scrape_list
        )

    def add_descriptions(self, metadata, identifier, args):
        return self.annotate_with_web_resources(
            metadata, identifier, args, self.summary_url,
            'No annotation info exists for this item',
            Hyperlink.DESCRIPTION, self._scrape_list
        )

    def add_author_notes(self, metadata, identifier, args):
        return self.annotate_with_web_resources(
            metadata, identifier, args, self.author_notes_url,
            'No author notes info exists for this item',
            Hyperlink.AUTHOR, self._scrape_one
        )

    def add_excerpt(self, metadata, identifier, args):
        return self.annotate_with_web_resources(
            metadata, identifier, args, self.excerpt_url,
            'No excerpt info exists for this item', Hyperlink.SAMPLE,
            self._scrape_one
        )

    def measure_popularity(self, identifier, cutoff=None):
        value = self.soap_client.estimated_popularity(
            identifier.identifier, cutoff=cutoff
        )
        # NOTE: even a complete lack of popularity data is useful--it tells
        # us there's no need to check again anytime soon. But we can't
        # store that information in the database.
        if value is not None:
            return MeasurementData(Measurement.POPULARITY, value)

    @classmethod
    def is_suitable_image(cls, image):
        """Is this a real cover image, or does it look like a stand-in image
        which should be ignored?
        """
        return not image.startswith(cls.STAND_IN_IMAGE_PREFIX)

    @classmethod
    def _scrape_list(cls, soup):
        resources = []
        table = soup.find('table', id='Table_Main')
        if table:
            for header in table.find_all('td', class_='SectionHeader'):
                content = header.parent.next_sibling
                if content.name != 'tr':
                    continue
                if not content.td:
                    continue
                resources.append(content.td.encode_contents())
        return resources

    @classmethod
    def _scrape_one(cls, soup):
        table = soup.find('table', id='Table_Main')
        resources = []
        if table and table.tr and table.tr.td:
            resources = [table.tr.td.encode_contents()]
        return resources

    @classmethod
    def _extract_title(cls, soup):
        """Find the book's title."""
        title_header = soup.find('span', class_='PageHeader2')
        if not title_header or not title_header.string:
            return
        title = title_header.string
        if title in cls.KNOWN_BAD_TITLES:
            title = None
        return title

class ContentCafeSOAPError(IOError):
    pass

class ContentCafeSOAPClient(object):
    """Get historical sales information from Content Cafe through
    its SOAP interface.

    NOTE: This class currently has no test coverage. It wouldn't be too
    difficult to add coverage for estimate_popularity, at least.
    """

    WSDL_URL = "http://contentcafe2.btol.com/ContentCafe/ContentCafe.asmx?WSDL"

    DEMAND_HISTORY = "DemandHistoryDetail"

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
        maximum all-time popularity, whichever is greater. If there
        are no measurements, returns None. This is different from a
        measurement of zero, which indicates a measured lack of
        demand.

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


class MockContentCafeAPI(ContentCafeAPI):

    def __init__(self, *args, **kwargs):
        self.requests = []
        self.responses = []
        self.measurements = []
        self.do_get = self._do_get

    def queue_response(self, response):
        self.responses.push(response)

    def queue_measurement(self, measurement):
        self.measurements.push(measurement)

    def _do_get(self, url):
        self.requests.push(url)
        return self.responses.pop()

    def measure_popularity(self, identifier, cutoff):
        return self.measurements.pop()
