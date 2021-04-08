from bs4 import BeautifulSoup
import datetime
import pytest
import os

from zeep.transports import Transport

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
from core.testing import (
    DummyHTTPClient,
    MockRequestsResponse,
)

from . import (
    DatabaseTest,
)
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider,
    ContentCafeSOAPClient,
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

class ContentCafeAPITest(object):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "content_cafe")

    def data_file(self, path):
        """Return the contents of a test data file."""
        return open(os.path.join(self.resource_path, path), 'rb').read()

class TestContentCafeAPI(ContentCafeAPITest, DatabaseTest):

    def setup_method(self):
        super(TestContentCafeAPI, self).setup_method()
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
        pytest.raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        # With incomplete integrations, an error is raised.
        integration = self._external_integration(
            ExternalIntegration.CONTENT_CAFE,
            goal=ExternalIntegration.METADATA_GOAL,
            username='yup'
        )
        pytest.raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = None
        integration.password = 'yurp'
        pytest.raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = 'yup'
        result = ContentCafeAPI.from_config(
            self._db, soap_client=object()
        )
        assert True == isinstance(result, ContentCafeAPI)

        # NOTE: We can't test the case where soap_client is not
        # mocked, because the ContentCafeSOAPClient constructor makes
        # a real HTTP request to load its WSDL file. We might be able
        # to improve this by seeing how mockable SudsClient is, or by
        # mocking ContentCafeAPISOAPClient.WSDL_URL as a file:// URL.

    def test_data_source(self):
        assert DataSource.CONTENT_CAFE == self.api.data_source.name

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
        assert None == m(self.identifier)
        request_url = self.http.requests.pop()
        image_url = api.image_url % self.args
        assert image_url == request_url
        assert [] == self.http.requests

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
        assert Hyperlink.IMAGE == image.rel
        assert image_url == image.href
        assert 'image/png' == image.media_type
        assert b'an image!' == image.content

        # We ran the image through our mocked version of is_suitable_image,
        # and it said it was fine.
        assert image.content == api.is_suitable_image_called_with

        # Here's the popularity measurement.
        assert [api.popularity_measurement] == metadata.measurements

        # Confirm that the mock methods were called with the right
        # arguments -- their functionality is tested individually
        # below.
        expected_args = (metadata, self.identifier, self.args)
        for called_with in (
            api.add_reviews_called_with, api.add_descriptions_called_with,
            api.add_author_notes_called_with, api.add_excerpt_called_with,
        ):
            assert expected_args == called_with
        assert ((self.identifier, api.ONE_YEAR_AGO),
            api.measure_popularity_called_with)

        # If measure_popularity returns nothing, metadata.measurements
        # will be left empty.
        api.popularity_measurement = None
        self.http.queue_requests_response(200, 'image/png', content='an image!')
        metadata = m(self.identifier)
        assert [] == metadata.measurements

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
        m(metadata, self.identifier, args, url_template, b"no data!", rel,
          scrapers.explode)
        # We made the request but nothing happened.
        expect_url = url_template % args
        assert expect_url == self.http.requests.pop()
        assert False == scrapers.explode_called
        assert None == metadata.title
        assert [] == metadata.links

        # Otherwise, we try to scrape.
        good_content = '<html><span class="PageHeader2">Book title</span><content>Here you go</content>'
        http.queue_requests_response(200, 'text/html', content=good_content)
        m(metadata, self.identifier, args, url_template, b"no data!", rel,
          scrapers.scrape)
        assert True == scrapers.scrape_called

        # We called _extract_title and took a Content Cafe title out
        # for the Metadata object.
        assert "Book title" == metadata.title

        # Then we called mock_scrape, which gave us the content for
        # one LinkData.
        [link] = metadata.links
        assert rel == link.rel
        assert None == link.href
        assert "text/html" == link.media_type
        assert "Here you go" == link.content

    def test__extract_title(self):
        # Standalone test of the _extract_title helper method.

        def assert_title(title, expect):
            markup = '<html><span class="PageHeader2">%s</span><content>Description</content>' % title
            soup = BeautifulSoup(markup, 'lxml')
            assert expect == ContentCafeAPI._extract_title(soup)


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
        assert 6 == len(reviews)
        assert all([x.rel==Hyperlink.REVIEW for x in reviews])
        assert "isn't a myth!" in reviews[0].content.decode("utf8")

        # We incidentally figured out the book's title.
        assert "Shadow Thieves" == metadata.title

    def test_add_author_notes(self):
        """Verify that add_author_notes works in a real case."""
        metadata = Metadata(DataSource.CONTENT_CAFE)
        content = self.data_file("author_notes.html")
        self.http.queue_requests_response(200, 'text/html', content=content)
        self.api.add_author_notes(metadata, self.identifier, self.args)

        [notes] = metadata.links
        assert Hyperlink.AUTHOR == notes.rel
        assert 'Brenda researched turtles' in notes.content.decode("utf8")

        # We incidentally figured out the book's title.
        assert "Franklin's Christmas Gift" == metadata.title

    def test_add_excerpt(self):
        """Verify that add_excerpt works in a real case."""
        metadata = Metadata(DataSource.CONTENT_CAFE)
        content = self.data_file("excerpt.html")
        self.http.queue_requests_response(200, 'text/html', content=content)
        self.api.add_excerpt(metadata, self.identifier, self.args)

        [excerpt] = metadata.links
        assert Hyperlink.SAMPLE == excerpt.rel
        assert 'Franklin loved his marbles.' in excerpt.content.decode("utf8")

        # We incidentally figured out the book's title.
        assert "Franklin's Christmas Gift" == metadata.title

    def test_measure_popularity(self):
        """Verify that measure_popularity turns the output of
        a SOAP request into a MeasurementData.
        """
        cutoff = object()

        # Call it.
        result = self.api.measure_popularity(self.identifier, cutoff)

        # The SOAP client's estimated_popularity method was called.
        expect = (self.identifier.identifier, cutoff)
        assert expect == self.soap.estimated_popularity_calls.pop()

        # The result was turned into a MeasurementData.
        assert isinstance(result, MeasurementData)
        assert Measurement.POPULARITY == result.quantity_measured
        assert self.soap.popularity_value == result.value

        # If the SOAP API doesn't return a popularity value, no
        # MeasurementData is created.
        self.soap.popularity_value = None
        result = self.api.measure_popularity(self.identifier, cutoff)
        assert expect == self.soap.estimated_popularity_calls.pop()
        assert None == result

    def test_is_suitable_image(self):
        # Images are rejected if we can tell they are Content Cafe's
        # stand-in images.
        m = ContentCafeAPI.is_suitable_image

        content = self.data_file("stand-in-image.png")
        assert False == m(content)

        # Otherwise, it's fine. We don't check that the image is
        # valid, only that it's not a stand-in image.
        assert True == m(b"I'm not a stand-in image.")


class TestContentCafeCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        mock_api = object()
        mock_mirror = object()
        policy = object()
        provider = ContentCafeCoverageProvider(
            self._default_collection, api=mock_api, replacement_policy=policy
        )
        assert self._default_collection == provider.collection
        assert policy == provider.replacement_policy
        assert mock_api == provider.content_cafe

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
        assert mock_api == provider.content_cafe

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
        assert identifier == provider.process_item(identifier)

        # An Edition has been created representing Content Cafe's
        # take on this book.
        [edition] = identifier.primarily_identifies
        assert DataSource.CONTENT_CAFE == edition.data_source.name

        # MockContentCafeAPI.create_metadata(identifier) was called.
        metadata = api.metadata
        assert identifier == metadata.identifier

        # And then apply() was called on the resulting MockMetadata
        # object.
        args, kwargs = metadata.apply_called_with
        assert (edition,) == args
        assert None == kwargs['collection']
        assert provider.replacement_policy == kwargs['replace']

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
        assert True == isinstance(result, CoverageFailure)
        assert identifier == result.obj
        assert ("Content Cafe has no knowledge of this identifier." ==
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
        assert True == isinstance(result, CoverageFailure)
        assert identifier == result.obj
        assert "Oh no!" in result.exception


class MockSession(DummyHTTPClient):
    """Mock the requests Session object used by zeep.transports.Transport."""

    def __init__(self, wsdl):
        super(MockSession, self).__init__()
        self.wsdl = wsdl
        self.headers = {}

    def get(self, address, *args, **kwargs):
        # No need to explicitly queue up requests for the WSDL URL --
        # we know they'll happen a lot.
        if address == ContentCafeSOAPClient.WSDL_URL:
            return MockRequestsResponse(200, {}, self.wsdl)
        return super(MockSession, self).do_get(address)

    def post(self, address, data, *args, **kwargs):
        return super(MockSession, self).do_post(address, data)


class TestContentCafeSOAPClient(ContentCafeAPITest):

    def setup_method(self):
        wsdl = self.data_file("service.wsdl")
        self.session = MockSession(wsdl)
        self.transport = Transport(session=self.session)
        self.client = ContentCafeSOAPClient(
            user_id="user id", password="password", transport=self.transport
        )


    def test_estimated_popularity_demand_when_info_present(self):
        # When Content Cafe has information about demand for the
        # requested book from various sources, we're able to get that
        # data through a SOAP request and tabulate it to estimate
        # total demand.
        info = self.data_file("demand_info_present.xml")
        def queue():
            # Queue up the demand info.
            self.session.queue_requests_response(
                200, "application/xml", content=info
            )
        queue()

        # Estimate this book's popularity as it was on the day the
        # data was gathered.
        data_gather_date = datetime.date(2019, 8, 4)
        popularity = self.client.estimated_popularity(
            "an isbn", as_of=data_gather_date
        )

        # Apart from the initial request to get the WSDL, a single
        # POST request was made.
        endpoint, post_data = self.session.requests.pop()
        assert [] == self.session.requests

        # The target of the POST request was the endpoint specified in
        # the WSDL file.
        assert ('http://contentcafe2.btol.com/ContentCafe/ContentCafe.asmx' ==
            endpoint)

        # The entity-body was an XML document invoking the
        # DemandHistoryDetail method with the arguments we expect.
        for must_include in (
            b'key>an isbn<',
            b'userID>user id<',
            b'password>password<',
            b'content>DemandHistoryDetail<',
        ):
                assert must_include in post_data

        # The answer given is the maximum total demand for a book in a
        # single recent month.
        base_popularity = 1347
        assert base_popularity == popularity

        # Now let's imagine that it's six months later and no further
        # popularity data has been gathered. This takes the period of
        # popularity past our threshold of relevancy -- which, let's
        # say, is three months.
        queue()
        six_months_later = data_gather_date + datetime.timedelta(days=180)
        three_months = datetime.timedelta(days=90)

        # The book's popularity is now given at half of its all-time
        # greatest popularity.  This reflects the fact that it used to
        # be very popular, and is probably still a book that library
        # patrons want to borrow.
        popularity = self.client.estimated_popularity(
            "an isbn", as_of=six_months_later, cutoff=three_months
        )
        assert base_popularity/2.0 == popularity

        # If our period of relevancy was longer, then six-month-old
        # data would still be relevant, and that number would be used
        # for its popularity
        queue()
        two_years = datetime.timedelta(days=365*2)
        popularity = self.client.estimated_popularity(
            "an isbn", as_of=six_months_later, cutoff=two_years
        )
        assert base_popularity == popularity

    def test_estimated_popularity_demand_when_info_absent(self):
        # When demand info is missing, the book's popularity is given
        # as None, representing a lack of data.  We don't get a crash.
        info = self.data_file("demand_info_missing.xml")

        self.session.queue_requests_response(
            200, "application/xml", content=info
        )
        assert None == self.client.estimated_popularity("an isbn")

