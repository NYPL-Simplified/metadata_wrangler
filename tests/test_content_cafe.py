from bs4 import BeautifulSoup

from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

import os

from core import mirror
from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.metadata_layer import (
    Metadata,
    MeasurementData,
)
from core.mirror import MirrorUploader
from core.model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Measurement,
)
from core.s3 import (
    MockS3Uploader,
    S3Uploader,
)
from core.testing import DummyHTTPClient

from . import (
    DatabaseTest,
)
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider,
)
import content_cafe

class MockSOAPClient(object):

    def __init__(self, popularity_value):
        # This value will be returned every time estimated_popularity
        # is called.
        self.popularity_value = popularity_value
        self.estimated_popularity_calls = []

    def estimated_popularity(self, identifier, cutoff):
        self.estimated_popularity_calls.append((identifier, cutoff))
        return self.popularity_value

class TestContentCafeAPI(DatabaseTest):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "content_cafe")

    def data_file(self, path):
        """Return the contents of a test data file."""
        return open(os.path.join(self.resource_path, path)).read()

    def setup(self):
        super(TestContentCafeAPI, self).setup()
        self.http = DummyHTTPClient()
        self.soap = MockSOAPClient(popularity_value=5)
        self.api = ContentCafeAPI(
            self._db, 'uid', 'pw', self.soap, self.http.do_get
        )
        self.identifier = self._identifier(identifier_type=Identifier.ISBN)
        self.args = dict(userid=self.api.user_id, password=self.api.password,
                         isbn=self.identifier.identifier)

    def test_from_config(self):
        # Without an integration, an error is raised.
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        # With incomplete integrations, an error is raised.
        integration = self._external_integration(
            ExternalIntegration.CONTENT_CAFE,
            goal=ExternalIntegration.METADATA_GOAL,
            username=u'yup'
        )
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = None
        integration.password = u'yurp'
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = u'yup'
        result = ContentCafeAPI.from_config(
            self._db, soap_client=object()
        )
        eq_(True, isinstance(result, ContentCafeAPI))

        # NOTE: We can't test the case where soap_client is not
        # mocked, because the ContentCafeSOAPClient constructor makes
        # a real HTTP request to load its WSDL file. We might be able
        # to improve this by seeing how mockable SudsClient is, or by
        # mocking ContentCafeAPISOAPClient.WSDL_URL as a file:// URL.

    def test_data_source(self):
        eq_(DataSource.CONTENT_CAFE, self.api.data_source.name)

    def test_create_metadata(self):

        class Mock(ContentCafeAPI):

            popularity_measurement = "a popularity measurement"
            annotate_calls = []

            def add_reviews(self, *args):
                self.add_reviews_called_with = args

            def add_descriptions(self, *args):
                self.add_descriptions_called_with = args

            def add_author_notes(self, *args):
                self.add_author_notes_called_with = args

            def add_excerpt(self, *args):
                self.add_excerpt_called_with = args

            def measure_popularity(self, *args):
                self.measure_popularity_called_with = args
                return self.popularity_measurement

            def is_suitable_image(self, image):
                self.is_suitable_image_called_with = image
                return True

        api = Mock(self._db, 'uid', 'pw', self.soap, self.http.do_get)
        m = api.create_metadata

        # First we will make a request for a cover image. If that
        # gives a 404 error, we return nothing and don't bother making
        # any more requests.
        self.http.queue_requests_response(404)
        eq_(None, m(self.identifier))
        request_url = self.http.requests.pop()
        image_url = api.image_url % self.args
        eq_(image_url, request_url)
        eq_([], self.http.requests)

        # If the cover image request succeeds, we turn it into a LinkData
        # and add it to a new Metadata object. We then pass the
        # Metadata object a number of other methods to get additional
        # information from Content Cafe.
        #
        # We then call measure_popularity, and add its return value
        # to Metadata.measurements.
        self.http.queue_requests_response(200, 'image/png', content='an image!')

        # Here's the result.
        metadata = m(self.identifier)

        # Here's the image LinkData.
        [image] = metadata.links
        eq_(Hyperlink.IMAGE, image.rel)
        eq_(image_url, image.href)
        eq_('image/png', image.media_type)
        eq_('an image!', image.content)

        # We ran the image through our mocked version of is_suitable_image,
        # and it said it was fine.
        eq_(image.content, api.is_suitable_image_called_with)

        # Here's the popularity measurement.
        eq_([api.popularity_measurement], metadata.measurements)

        # Confirm that the mock methods were called with the right
        # arguments -- their functionality is tested individually
        # below.
        expected_args = (metadata, self.identifier, self.args)
        for called_with in (
            api.add_reviews_called_with, api.add_descriptions_called_with,
            api.add_author_notes_called_with, api.add_excerpt_called_with,
        ):
            eq_(expected_args, called_with)
        eq_((self.identifier, api.ONE_YEAR_AGO),
            api.measure_popularity_called_with)

        # If measure_popularity returns nothing, metadata.measurements
        # will be left empty.
        api.popularity_measurement = None
        self.http.queue_requests_response(200, 'image/png', content='an image!')
        metadata = m(self.identifier)
        eq_([], metadata.measurements)

    def test_annotate_with_web_resources(self):
        metadata = Metadata(DataSource.CONTENT_CAFE)
        rel = self._str

        # We're going to be grabbing this URL and
        # scraping it.
        url_template = "http://url/%(arg1)s"
        args = dict(arg1='value')

        # A couple of useful functions for scraping.
        class MockScrapers(object):
            scrape_called = False
            explode_called = False
            def scrape(self, soup):
                self.scrape_called = True
                return [soup.find('content').string]

            def explode(self, soup):
                self.explode_called = True
                raise Exception("I'll never be called")
        scrapers = MockScrapers()

        # When the result of the HTTP request contains a certain phrase,
        # we don't even bother scraping.
        m = self.api.annotate_with_web_resources
        http = self.http
        http.queue_requests_response(
            200, 'text/html', content='There is no data!'
        )
        m(metadata, self.identifier, args, url_template, "no data!", rel,
          scrapers.explode)
        # We made the request but nothing happened.
        expect_url = url_template % args
        eq_(expect_url, self.http.requests.pop())
        eq_(False, scrapers.explode_called)
        eq_(None, metadata.title)
        eq_([], metadata.links)

        # Otherwise, we try to scrape.
        good_content = '<html><span class="PageHeader2">Book title</span><content>Here you go</content>'
        http.queue_requests_response(200, 'text/html', content=good_content)
        m(metadata, self.identifier, args, url_template, "no data!", rel,
          scrapers.scrape)
        eq_(True, scrapers.scrape_called)

        # We called _extract_title and took a Content Cafe title out
        # for the Metadata object.
        eq_("Book title", metadata.title)

        # Then we called mock_scrape, which gave us the content for
        # one LinkData.
        [link] = metadata.links
        eq_(rel, link.rel)
        eq_(None, link.href)
        eq_("text/html", link.media_type)
        eq_("Here you go", link.content)

    def test__extract_title(self):
        # Standalone test of the _extract_title helper method.

        def assert_title(title, expect):
            markup = '<html><span class="PageHeader2">%s</span><content>Description</content>' % title
            soup = BeautifulSoup(markup, 'lxml')
            eq_(expect, ContentCafeAPI._extract_title(soup))


        # A normal book title is successfully extracted.
        assert_title("A great book", "A great book")

        # A supposed title that's in KNOWN_BAD_TITLES is ignored.
        assert_title("No content currently exists for this item", None)

    def test_add_reviews(self):
        """Verify that add_reviews works in a real case."""
        metadata = Metadata(DataSource.CONTENT_CAFE)
        content = self.data_file("reviews.html")
        self.http.queue_requests_response(200, 'text/html', content=content)
        self.api.add_reviews(metadata, self.identifier, self.args)

        # We extracted six reviews from the sample file.
        reviews = metadata.links
        eq_(6, len(reviews))
        assert all([x.rel==Hyperlink.REVIEW for x in reviews])
        assert "isn't a myth!" in reviews[0].content

        # We incidentally figured out the book's title.
        eq_("Shadow Thieves", metadata.title)

    def test_add_author_notes(self):
        """Verify that add_author_notes works in a real case."""
        metadata = Metadata(DataSource.CONTENT_CAFE)
        content = self.data_file("author_notes.html")
        self.http.queue_requests_response(200, 'text/html', content=content)
        self.api.add_author_notes(metadata, self.identifier, self.args)

        [notes] = metadata.links
        eq_(Hyperlink.AUTHOR, notes.rel)
        assert 'Brenda researched turtles' in notes.content

        # We incidentally figured out the book's title.
        eq_("Franklin's Christmas Gift", metadata.title)

    def test_add_excerpt(self):
        """Verify that add_excerpt works in a real case."""
        metadata = Metadata(DataSource.CONTENT_CAFE)
        content = self.data_file("excerpt.html")
        self.http.queue_requests_response(200, 'text/html', content=content)
        self.api.add_excerpt(metadata, self.identifier, self.args)

        [excerpt] = metadata.links
        eq_(Hyperlink.SAMPLE, excerpt.rel)
        assert 'Franklin loved his marbles.' in excerpt.content

        # We incidentally figured out the book's title.
        eq_("Franklin's Christmas Gift", metadata.title)

    def test_measure_popularity(self):
        """Verify that measure_popularity turns the output of
        a SOAP request into a MeasurementData.
        """
        cutoff = object()

        # Call it.
        result = self.api.measure_popularity(self.identifier, cutoff)

        # The SOAP client's estimated_popularity method was called.
        expect = (self.identifier.identifier, cutoff)
        eq_(expect, self.soap.estimated_popularity_calls.pop())

        # The result was turned into a MeasurementData.
        assert isinstance(result, MeasurementData)
        eq_(Measurement.POPULARITY, result.quantity_measured)
        eq_(self.soap.popularity_value, result.value)

        # If the SOAP API doesn't return a popularity value, no
        # MeasurementData is created.
        self.soap.popularity_value = None
        result = self.api.measure_popularity(self.identifier, cutoff)
        eq_(expect, self.soap.estimated_popularity_calls.pop())
        eq_(None, result)

    def test_is_suitable_image(self):
        # Images are rejected if we can tell they are Content Cafe's
        # stand-in images.
        m = ContentCafeAPI.is_suitable_image

        content = self.data_file("stand-in-image.png")
        eq_(False, m(content))

        # Otherwise, it's fine. We don't check that the image is
        # valid, only that it's not a stand-in image.
        eq_(True, m("I'm not a stand-in image."))


class TestContentCafeCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        mock_api = object()
        mock_mirror = object()
        policy = object()
        provider = ContentCafeCoverageProvider(
            self._default_collection, api=mock_api, replacement_policy=policy
        )
        eq_(self._default_collection, provider.collection)
        eq_(policy, provider.replacement_policy)
        eq_(mock_api, provider.content_cafe)

        # If no ContentCafeAPI is provided, the output of
        # ContentCafeAPI.from_config is used.
        class MockContentCafeAPI(ContentCafeAPI):
            @classmethod
            def from_config(cls, *args, **kwargs):
                return mock_api
        content_cafe.ContentCafeAPI = MockContentCafeAPI

        # Now we can invoke the constructor with no special arguments
        # and our mocked default will be used.
        provider = ContentCafeCoverageProvider(self._default_collection)
        eq_(mock_api, provider.content_cafe)

        # Restore mocked classes
        content_cafe.ContentCafeAPI = ContentCafeAPI

    def test_process_item_success(self):
        class MockMetadata(object):
            def __init__(self, identifier):
                self.identifier = identifier

            def apply(self, *args, **kwargs):
                self.apply_called_with = (args, kwargs)

        class MockContentCafeAPI(object):
            """Pretend that we went to Content Cafe and got some Metadata."""
            def create_metadata(self, identifier):
                self.metadata = MockMetadata(identifier)
                return self.metadata
        api = MockContentCafeAPI()

        provider = ContentCafeCoverageProvider(
            self._default_collection, api
        )
        identifier = self._identifier()

        # process_item indicates success by returning the Identifier
        # it was given.
        eq_(identifier, provider.process_item(identifier))

        # An Edition has been created representing Content Cafe's
        # take on this book.
        [edition] = identifier.primarily_identifies
        eq_(DataSource.CONTENT_CAFE, edition.data_source.name)

        # MockContentCafeAPI.create_metadata(identifier) was called.
        metadata = api.metadata
        eq_(identifier, metadata.identifier)

        # And then apply() was called on the resulting MockMetadata
        # object.
        args, kwargs = metadata.apply_called_with
        eq_((edition,), args)
        eq_(None, kwargs['collection'])
        eq_(provider.replacement_policy, kwargs['replace'])

    def test_process_item_failure_not_found(self):
        """Test what happens when Content Cafe hasn't heard of
        an Identifier.
        """

        class NotFoundContentAPI(object):
            def create_metadata(self, *args, **kwargs):
                return None

        provider = ContentCafeCoverageProvider(
            self._default_collection, api=NotFoundContentAPI(),
            replacement_policy=object()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        eq_("Content Cafe has no knowledge of this identifier.",
            result.exception)

    def test_process_item_exception(self):
        """Test what happens when an exception is raised
        in the course of obtaining coverage.
        """
        class CantCreateMetadata(object):
            def create_metadata(self, *args, **kwargs):
                raise Exception("Oh no!")

        provider = ContentCafeCoverageProvider(
            self._default_collection, api=CantCreateMetadata(),
            replacement_policy=object()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        assert "Oh no!" in result.exception
