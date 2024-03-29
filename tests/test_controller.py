# encoding: utf-8
from contextlib import contextmanager
import base64 as stdlib_base64
import os
import feedparser
import json
import re
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA
from Crypto.Signature import PKCS1_v1_5
from Crypto.PublicKey import RSA
from io import BytesIO
from datetime import datetime, timedelta
from functools import wraps
import jwt
from pdb import set_trace
import flask

from lxml import etree

from . import (
    DatabaseTest,
    sample_data
)
from core.config import Configuration
from core.lane import Pagination
from core.model import (
    Collection,
    ConfigurationSetting,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    IntegrationClient,
    Work,
    get_one,
)
from core.opds import AcquisitionFeed
from core.opds_import import OPDSXMLParser
from core.overdrive import MockOverdriveAPI
from core.s3 import MockS3Uploader
from core.testing import (
    DummyHTTPClient,
    MockRequestsResponse,
)
from core.util.problem_detail import ProblemDetail
from core.util.opds_writer import OPDSMessage
from core.util.string_helpers import base64
from core.util.datetime_helpers import utc_now

from canonicalize import (
    AuthorNameCanonicalizer,
    SimpleMockAuthorNameCanonicalizer,
)
from content_cafe import (
    ContentCafeCoverageProvider,
    MockContentCafeAPI,
)
from controller import (
    MetadataWrangler,
    CanonicalizationController,
    CatalogController,
    Controller,
    IndexController,
    IntegrationClientController,
    URNLookupController,
    URNLookupHandler,
    HTTP_OK,
    HTTP_CREATED,
    HTTP_ACCEPTED,
    HTTP_UNAUTHORIZED,
    HTTP_NOT_FOUND,
    HTTP_INTERNAL_SERVER_ERROR,
)
from coverage_provider import (
    IdentifierResolutionCoverageProvider,
)
from integration_client import IntegrationClientCoverImageCoverageProvider
from overdrive import (
    OverdriveBibliographicCoverageProvider,
)

from problem_details import *
from viaf import MockVIAFClient

def unauthenticated_request_context(f):
    """A decorator for a test that runs in a simple request context
    with no authentication.

    This is useful if a test runs in one unauthenticated request that
    does nothing special.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from app import app
        with app.test_request_context('/'):
            return f(*args, **kwargs)
    return decorated

def authenticated_request_context(f):
    """A decorator for a test that runs in a simple request context
    with valid authentication.

    This is useful if a test runs in one authenticated request that
    does nothing special.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from app import app

        secret = args[0].client.shared_secret.encode('utf8')
        valid_auth = 'Bearer '+ base64.urlsafe_b64encode(secret)
        headers = { 'Authorization' : valid_auth }
        with app.test_request_context('/', headers=headers):
            return f(*args, **kwargs)
    return decorated


class ControllerTest(DatabaseTest):

    def setup_method(self):
        super(ControllerTest, self).setup_method()

        from app import app
        self.app = app

        # Set up an IntegrationClient and a sample set of HTTP headers
        # for authenticating as that client.
        self.client = self._integration_client()
        self.valid_auth = 'Bearer ' + base64.b64encode(self.client.shared_secret)

    def sample_data(self, filename):
        return sample_data(filename, 'controller')

    @contextmanager
    def authenticated_request(self, *args, **kwargs):
        """Set up a test request context with an Authentication
        header that identifies the IntegrationClient initialized in
        setup_method().

        This is useful when you need something more complicated than
        authenticated_request_context.
        """
        headers = kwargs.pop("headers", {})
        headers['Authorization'] = self.valid_auth
        with self.app.test_request_context(
            *args, headers=headers, **kwargs
        ) as c:
            yield c


class TestMetadataWrangler(ControllerTest):
    """Tests of the MetadataWrangler class."""

    @authenticated_request_context
    def test_valid_authentication(self):
        # If the credentials are valid, an IntegrationClient is
        # returned and set to flask.request.authenticated_client.
        result = MetadataWrangler.authenticated_client_from_request(self._db)
        assert result == self.client
        assert self.client == flask.request.authenticated_client

    @authenticated_request_context
    def test_disabled_client(self):
        self.client.enabled = False
        # If the credentials are valid but the IntegrationClient is
        # disabled, DISABLED_CLIENT is returned.
        result = MetadataWrangler.authenticated_client_from_request(self._db)
        assert DISABLED_CLIENT == result

    def test_invalid_authentication(self):
        # If the credentials are missing or invalid, but
        # authentication is required, a ProblemDetail is returned.
        m = MetadataWrangler.authenticated_client_from_request

        # Test various invalid authentications.
        invalid_bearer = 'Bearer ' + base64.b64encode('wrong_secret')
        invalid_basic = 'Basic ' + base64.b64encode('abc:defg')
        not_base64_encoded = 'Bearer ' + self.client.shared_secret
        invalid = [
            dict(Authorization=x)
            for x in (invalid_bearer, invalid_basic, not_base64_encoded)
        ]

        # Invalid credentials result in INVALID_CREDENTIALS whether or
        # not authentication is required.
        for invalid_auth in invalid:
            with self.app.test_request_context('/', headers=invalid_auth):
                for required in True, False:
                    result = m(self._db, required)
                    assert INVALID_CREDENTIALS == result
                    assert None == flask.request.authenticated_client

        # Missing credentials result in INVALID_CREDENTIALS only if
        # authentication is required. If authentication is not
        # required, authenticated_client_from_request returns None.
        with self.app.test_request_context('/', headers={}):
            result = m(self._db, required=True)
            assert INVALID_CREDENTIALS == result
            assert None == flask.request.authenticated_client

            result = m(self._db, required=False)
            assert None == result
            assert None == flask.request.authenticated_client


class TestController(ControllerTest):

    def setup_method(self):
        super(TestController, self).setup_method()
        self.controller = Controller(self._db)

    def test_default_collection(self):
        # The default collection is the "unaffiliated" collection associated with
        # the IdentifierResolutionCoverageProvider.
        unaffiliated, ignore = IdentifierResolutionCoverageProvider.unaffiliated_collection(self._db)
        assert unaffiliated == self.controller.default_collection

    def test_load_collection_success(self):
        # Over on a circulation manager, we have an Overdrive collection.
        remote_collection = self._collection(
            external_account_id=self._str,
            protocol=ExternalIntegration.OVERDRIVE
        )
        # A metadata_identifier can be calculated for it.
        metadata_identifier = remote_collection.metadata_identifier

        # Let's look up the corresponding Collection on this metadata
        # wrangler.
        url = '/?data_source=Some%20data%20source'
        with self.authenticated_request(url):

            # A new collection is created, named after the metadata identifier.
            collection = self.controller.load_collection(metadata_identifier)
            assert collection.name == remote_collection.metadata_identifier

            # The metadata wrangler collection has the same protocol as the
            # one on the circulation manager.
            assert remote_collection.protocol == collection.protocol

            # But it has no external_account_id, because it uses the
            # Overdrive protocol, and we only care when the protocol
            # is "OPDS Import".
            assert None == collection.external_account_id

            # Its data_source is Overdrive -- that's where Overdrive
            # collections are from -- not the string we passed in as
            # the data_source request parameter.
            assert DataSource.OVERDRIVE == collection.data_source.name

            # This is a brand new collection. It's not
            # remote_collection. That's because remote_collection
            # isn't _named_ after its metadata identifier. This isn't
            # ideal, but it doesn't matter -- remember that
            # remote_collection is 'really' on another server, a
            # circulation manager.
            assert remote_collection != collection

        with self.authenticated_request(url):
            # Look up the metadata identifier again, and the previously
            # created collection is reused.
            collection2 = self.controller.load_collection(metadata_identifier)
            assert collection2 == collection

        with self.authenticated_request(url):
            # Now try to look up an OPDS Import collection -- those are
            # handled a little differently.
            remote_collection = self._collection(
                external_account_id=self._url,
                protocol=ExternalIntegration.OPDS_IMPORT
            )
            metadata_identifier = remote_collection.metadata_identifier

            collection = self.controller.load_collection(metadata_identifier)

        # Here, external_account_id is set to the value found on the
        # remote collection, and data_source is set to a new DataSource based on
        # the argument passed through the URL.
        assert metadata_identifier == collection.name
        assert ExternalIntegration.OPDS_IMPORT == collection.protocol
        assert remote_collection.external_account_id == collection.external_account_id
        assert "Some data source" == collection.data_source.name

    @unauthenticated_request_context
    def test_load_collection_unauthenticated(self):
        # If no authentication is provided, then you can't look up a
        # specific collection.

        remote_collection = self._collection(
            external_account_id=self._str,
            protocol=ExternalIntegration.OVERDRIVE
        )
        metadata_identifier = remote_collection.metadata_identifier

        # load_collection() will return a ProblemDetail if called with
        # authentication_required=True, and the default collection if
        # called with authentication_required=False.
        collection = self.controller.load_collection(metadata_identifier)
        assert INVALID_CREDENTIALS == collection

        collection = self.controller.load_collection(
            metadata_identifier, authentication_required=False
        )
        assert self.controller.default_collection == collection

    @authenticated_request_context
    def test_load_collection_failure(self):
        # If you don't provide a valid metadata identifier, you get a
        # ProblemDetail.

        collection = None

        # No input.
        result = self.controller.load_collection("")
        assert INVALID_INPUT.uri == result.uri
        assert "No metadata identifier provided." == result.detail

        # Invalid input.
        result = self.controller.load_collection("not a real metadata identifier")
        assert INVALID_INPUT.uri == result.uri
        assert ("Metadata identifier 'not a real metadata identifier' is invalid: Incorrect padding" ==
            result.detail)


class TestIndexController(ControllerTest):

    def test_opds_catalog(self):
        controller = IndexController(self._db)
        request_url = "http://localhost/give-me-opds-catalog"
        with self.app.test_request_context(request_url):
            response = controller.opds_catalog()

        assert 200 == response.status_code
        catalog = json.loads(response.data)

        # In the absence of a configured BASE_URL, the ID of the
        # OPDS catalog is the request URL.
        assert request_url == catalog.get('id')

        urls = [l.get('href') for l in catalog.get('links')]
        # Use flask endpoint syntax for path variables
        urls = [re.sub('\{', '<', url) for url in urls]
        urls = [re.sub('\}', '>', url) for url in urls]
        # Remove arguments from templated urls
        urls = [re.sub('<\?[\w,_]*\*?>', '', url) for url in urls]

        # Compare the catalogued urls with the app endpoints.
        endpoints = [r.rule for r in self.app.url_map.iter_rules()]
        for url in urls:
            assert url in endpoints

    def test_opds_catalog_application_id(self):
        controller = IndexController(self._db)

        # Configure a base URL.
        app_url = self._url
        ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY).value = app_url
        with self.app.test_request_context('/give-me-opds-catalog'):
            response = controller.opds_catalog()
        catalog = json.loads(response.data)

        # Unlike the previous test, the ID of the OPDS catalog is the
        # BASE_URL, not the URL used in the request.
        assert app_url == catalog.get('id')


class TestCatalogController(ControllerTest):

    XML_PARSE = OPDSXMLParser()._xpath

    def setup_method(self):
        super(TestCatalogController, self).setup_method()
        self.controller = CatalogController(self._db)

        # The collection as it exists on the circulation manager.
        remote_collection = self._collection(
            username='test_coll', external_account_id=self._url,
        )
        # The collection as it is recorded / catalogued here.
        self.collection = self._collection(
            name=remote_collection.metadata_identifier,
            protocol=remote_collection.protocol
        )

        # Create two works to use in tests.
        self.work1 = self._work(with_open_access_download=True)
        self.work2 = self._work(with_open_access_download=True)

        # Clear the cached OPDS entries for one of these works -- this
        # verifies that OPDS entries are created when needed.
        self.work1.verbose_opds_entry = None
        self.work1.simple_opds_entry = None

    @classmethod
    def get_root(cls, raw_feed):
        """Returns the root tag of an OPDS or XML feed."""
        return etree.parse(BytesIO(raw_feed))

    @classmethod
    def get_messages(cls, root):
        """Returns the OPDSMessages from a feed, given its root tag."""
        message_path = '/atom:feed/simplified:message'
        if isinstance(root, (bytes, str)):
            root = cls.get_root(root)
        return cls.XML_PARSE(root, message_path)

    @classmethod
    def xml_value(cls, message, tag):
        return cls.XML_PARSE(message, tag)[0].text

    @classmethod
    def get_message_for(cls, identifier, messages):
        if not isinstance(identifier, (bytes, str)):
            identifier = identifier.urn
        [message] = [m for m in messages
                     if cls.xml_value(m, 'atom:id')==identifier]
        return message

    @classmethod
    def assert_message(cls, message, identifier, status_code, description):
        if isinstance(message, list):
            message = cls.get_message_for(identifier, message)
        assert str(status_code) == cls.xml_value(message, 'simplified:status_code')
        assert description == cls.xml_value(message, 'schema:description')

    @unauthenticated_request_context
    def test_collection_feed_url(self):
        # A basic url can be created with the collection details.
        result = self.controller.collection_feed_url(
            'add', self.collection
        )
        assert result.endswith('/%s/add' % self.collection.name)

        # A url with parameters includes them in the url.
        result = self.controller.collection_feed_url(
            'add', self.collection, urn=['bananas', 'lol'],
            last_update_time='what'
        )
        assert '/%s/add?' % self.collection.name in result
        assert 'urn=bananas' in result
        assert 'urn=lol' in result
        assert 'last_update_time=what' in result

        # If a Pagination object is provided, its details are included.
        page = Pagination(offset=3, size=25)
        result = self.controller.collection_feed_url(
            'remove', self.collection, page=page, urn='unicorn',
        )
        assert '/%s/remove?' % self.collection.name in result
        assert 'urn=unicorn' in result
        assert 'after=3' in result
        assert 'size=25' in result

    def test_add_pagination_links_to_feed(self):
        query = self._db.query(Work).limit(2)
        page = Pagination(offset=0, size=1)
        feed = AcquisitionFeed(self._db, 'Hi', self._url, [])

        # The first page has the 'next' link.
        with self.app.test_request_context('/'):
            self.controller.add_pagination_links_to_feed(
                page, query, feed, 'add', self.collection
            )

        # The feed has the expected links.
        links = feedparser.parse(str(feed)).feed.links
        assert 2 == len(links)
        assert ['next', 'self'], sorted([l.rel for l in links])
        [next_href] = [l.href for l in links if l.rel=='next']
        assert 'after=1' in next_href
        assert 'size=1' in next_href

        # The url is collection-specific.
        assert self.collection.name+'/add' in next_href

        # The second page has the 'previous' and 'first' links.
        page = Pagination(offset=1, size=1)
        feed = AcquisitionFeed(self._db, 'Hi', self._url, [])
        with self.app.test_request_context('/'):
            self.controller.add_pagination_links_to_feed(
                page, query, feed, 'remove', self.collection,
                thing='whatever'
            )

        links = feedparser.parse(str(feed)).feed.links
        assert 3 == len(links)
        assert ['first', 'previous', 'self'] == sorted([l.rel for l in links])

        [first_href] = [l.href for l in links if l.rel=='first']
        [previous_href] = [l.href for l in links if l.rel=='previous']

        for href in [first_href, previous_href]:
            assert 'after=0' in href
            assert 'size=1' in href
            # The urls are collection-specific.
            assert self.collection.name+'/remove' in href

    def test_updates_feed(self):
        # Add an Identifier associated with a Work to our catalog.
        identifier = self.work1.license_pools[0].identifier
        self.collection.catalog_identifier(identifier)

        # Unauthenticated requests are rejected.
        with self.app.test_request_context('/'):
            response = self.controller.updates_feed(self.collection.name)
            assert INVALID_CREDENTIALS == response

        # Ask for updates as though the circulation manager had no
        # information about what's in its catalog.
        with self.authenticated_request('/'):
            response = self.controller.updates_feed(self.collection.name)
            # The catalog's updates feed is returned.
            assert HTTP_OK == response.status_code
            feed = feedparser.parse(response.get_data())
            assert (feed.feed.title ==
                "%s Collection Updates for %s" % (self.collection.protocol, self.client.url))

            # The feed has information on the only work in our
            # catalog.
            assert 1 == len(feed['entries'])
            [entry] = feed['entries']
            assert self.work1.title == entry['title']
            assert identifier.urn == entry['id']

        # We can ask for updates since a given time.
        yesterday = utc_now() - timedelta(days=1)
        yesterday_timestamp = yesterday.strftime(
            self.controller.TIMESTAMP_FORMAT
        )
        tomorrow = utc_now() + timedelta(days=1)
        tomorrow_timestamp = tomorrow.strftime(
            self.controller.TIMESTAMP_FORMAT
        )

        # If we ask for updates since before work1 was created,
        # we'll get work1.
        with self.authenticated_request(
                '/?last_update_time=%s' % yesterday_timestamp,
        ):
            response = self.controller.updates_feed(self.collection.name)
            assert HTTP_OK == response.status_code
            feed = feedparser.parse(response.get_data())
            assert (feed.feed.title ==
                "%s Collection Updates for %s" % (self.collection.protocol, self.client.url))

            # The timestamp is included in the url.
            linkified_timestamp = yesterday_timestamp.replace(":", "%3A")
            assert feed['feed']['id'].endswith(linkified_timestamp)
            assert 1 == len(feed['entries'])

        # If we ask for updates from a time later than work1 was created,
        # we'll get no results.
        with self.authenticated_request(
                '/?last_update_time=%s' % tomorrow_timestamp,
        ):
            response = self.controller.updates_feed(self.collection.name)
            assert HTTP_OK == response.status_code
            feed = feedparser.parse(response.get_data())
            assert 0 == len(feed['entries'])

        # The feed can be paginated.
        identifier2 = self.work2.license_pools[0].identifier
        self.collection.catalog_identifier(identifier2)
        with self.authenticated_request(
                '/?last_update_time=%s&size=1' % yesterday_timestamp,
        ):
            response = self.controller.updates_feed(self.collection.name)
            assert HTTP_OK == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']

            # work1 shows up first since it was created earlier.
            assert identifier.urn == entry['id']

            # The first page lists the total number of items in the catalog.
            assert '2' == feed['feed']['opensearch_totalresults']

        # Page two contains work2.
        with self.authenticated_request(
                '/?last_update_time=%s&size=1&after=1' % yesterday_timestamp,
        ):
            response = self.controller.updates_feed(self.collection.name)
            assert HTTP_OK == response.status_code
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            assert identifier2.urn == entry['id']

            # Subsequent pages don't bother listing the total number
            # of items.
            assert 'opensearch_totalresults' not in feed['feed']

    def test_updates_feed_is_paginated(self):
        for work in [self.work1, self.work2]:
            self.collection.catalog_identifier(work.license_pools[0].identifier)
        with self.authenticated_request('/?size=1'):
            response = self.controller.updates_feed(self.collection.name)
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'next' for link in links])
            assert not any([link['rel'] == 'previous' for link in links])
            assert not any([link['rel'] == 'first' for link in links])

        with self.authenticated_request('/?size=1&after=1'):
            response = self.controller.updates_feed(self.collection.name)
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'previous' for link in links])
            assert any([link['rel'] == 'first' for link in links])
            assert not any([link['rel'] == 'next'for link in links])

    def test_updates_feed_bad_last_update_time(self):
        """Passing in a malformed timestamp for last_update_time
        results in a problem detail document.
        """
        with self.authenticated_request('/?last_update_time=wrong format'):
            response = self.controller.updates_feed(self.collection.name)
            assert isinstance(response, ProblemDetail)
            assert 400 == response.status_code
            expect_error = 'The timestamp "wrong format" is not in the expected format (%s)' % self.controller.TIMESTAMP_FORMAT
            assert expect_error == response.detail

    def test_add_items(self):
        invalid_urn = "FAKE AS I WANNA BE"
        catalogued_id = self._identifier()
        uncatalogued_id = self._identifier()
        self.collection.catalog_identifier(catalogued_id)

        other_collection = self._collection()

        # Unauthenticated requests are rejected.
        with self.app.test_request_context('/'):
            response = self.controller.add_items(self.collection.name)
            assert INVALID_CREDENTIALS == response

        with self.authenticated_request(
                '/?urn=%s&urn=%s&urn=%s' % (
                catalogued_id.urn, uncatalogued_id.urn, invalid_urn),
                method='POST'
        ):
            response = self.controller.add_items(self.collection.name)

        # None of the identifiers raise or return an error.
        assert HTTP_OK == response.status_code

        # It sends three messages.
        m = messages = self.get_messages(response.get_data())
        assert 3 == len(messages)

        # The uncatalogued identifier is now in the catalog.
        assert uncatalogued_id in self.collection.catalog
        # It has an accurate response message.
        self.assert_message(m, uncatalogued_id, 201, 'Successfully added')

        # The catalogued identifier is still in the catalog.
        assert catalogued_id in self.collection.catalog
        # And even though it responds 'OK', the message tells you it
        # was already there.
        self.assert_message(m, catalogued_id, 200, 'Already in catalog')

        # The other catalog is not affected.
        assert [] == other_collection.catalog

        # Invalid identifier return 400 errors.
        self.assert_message(m, invalid_urn, 400, 'Could not parse identifier.')

    def test_add_with_metadata(self):
        # Unauthenticated requests are rejected.
        with self.app.test_request_context('/'):
            response = self.controller.add_with_metadata(self.collection.name)
            assert INVALID_CREDENTIALS == response

        # Pretend this OPDS came from a circulation manager.
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "opds")
        path = os.path.join(resource_path, "content_server_lookup.opds")
        opds = open(path).read()

        # Give the collection an OPDS_FOR_DISTRIBUTORS protocol to test
        # registration for cover mirroring.
        self.collection.protocol = ExternalIntegration.OPDS_FOR_DISTRIBUTORS

        # And here's some OPDS with an invalid identifier.
        invalid_opds = "<feed><entry><id>invalid</id></entry></feed>"

        with self.authenticated_request(data=opds):
            response = self.controller.add_with_metadata(self.collection.name)

        assert HTTP_OK == response.status_code

        # It sends one message.
        [catalogued] = self.get_messages(response.get_data())

        # The identifier in the OPDS feed is now in the catalog.
        identifier = self._identifier(foreign_id='20201')
        assert identifier in self.collection.catalog

        # It has an accurate response message.
        self.assert_message(catalogued, identifier, '201', 'Successfully added')

        # The identifier has links for the cover images from the feed.
        assert (set(["http://s3.amazonaws.com/book-covers.nypl.org/Gutenberg%20Illustrated/20201/cover_20201_0.png",
                 "http://s3.amazonaws.com/book-covers.nypl.org/Gutenberg%20Illustrated/20201/cover_20201_0.png"]) ==
            set([link.resource.url for link in identifier.links]))
        assert (set([Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]) ==
            set([link.rel for link in identifier.links]))

        # The identifier also has an Edition with title, author, and language.
        edition = get_one(self._db, Edition, primary_identifier=identifier)
        assert "Mary Gray" == edition.title
        [author] = edition.contributors
        assert Edition.UNKNOWN_AUTHOR == author.sort_name
        assert "eng" == edition.language

        # A DataSource was created for the collection.
        data_source = DataSource.lookup(self._db, self.collection.name)
        assert isinstance(data_source, DataSource)

        # If we make the same request again, the identifier stays in the catalog.
        with self.authenticated_request(data=opds):
            response = self.controller.add_with_metadata(self.collection.name)

        assert HTTP_OK == response.status_code

        # It sends one message.
        root = etree.parse(BytesIO(response.data))
        [catalogued] = self.get_messages(response.get_data())

        # The identifier in the OPDS feed is still in the catalog.
        assert identifier in self.collection.catalog

        # And even though it responds 'OK', the message tells you it
        # was already there.
        self.assert_message(catalogued, identifier, '200', 'Already in catalog')

        # The invalid identifier returns a 400 error message.
        with self.authenticated_request(data=invalid_opds):
            response = self.controller.add_with_metadata(self.collection.name)
        assert HTTP_OK == response.status_code

        # It sends one message.
        [invalid] = self.get_messages(response.get_data())
        self.assert_message(
            invalid, 'invalid', 400, 'Could not parse identifier.'
        )

    def test_metadata_needed_for(self):
        # Unauthenticated requests are rejected.
        with self.app.test_request_context('/'):
            response = self.controller.metadata_needed_for(self.collection.name)
            assert INVALID_CREDENTIALS == response

        # A regular schmegular identifier: untouched, pure.
        pure_id = self._identifier()

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        resolver = IdentifierResolutionCoverageProvider
        source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        resolved_id = self._identifier()
        self._coverage_record(resolved_id, source, operation=resolver.OPERATION)

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = self._identifier()
        self._coverage_record(
            unresolved_id, source, operation=resolver.OPERATION,
            status=CoverageRecord.TRANSIENT_FAILURE,
        )

        # An unresolved identifier that already has metadata waiting
        # for the IntegrationClientCoverageRecord.
        metadata_already_id = self._identifier()
        collection_source = DataSource.lookup(
            self._db, self.collection.name, autocreate=True
        )
        self._coverage_record(
            metadata_already_id, source, operation=resolver.OPERATION,
            status=CoverageRecord.TRANSIENT_FAILURE,
        )
        self._coverage_record(
            metadata_already_id, collection_source,
            operation=IntegrationClientCoverImageCoverageProvider.OPERATION,
            status=CoverageRecord.REGISTERED,
        )

        # An identifier with a Work already.
        id_with_work = self._work().presentation_edition.primary_identifier

        self.collection.catalog_identifiers([
            pure_id, resolved_id, unresolved_id, id_with_work,
            metadata_already_id,
        ])

        with self.authenticated_request():
            response = self.controller.metadata_needed_for(self.collection.name)

        [m] = self.get_messages(response.get_data())

        # Only the failing identifier that doesn't have metadata submitted yet
        # is in the feed.
        self.assert_message(m, unresolved_id, 202, 'Metadata needed.')

    def test_remove_items(self):
        # Unauthenticated requests are rejected.
        with self.app.test_request_context('/'):
            response = self.controller.remove_items(self.collection.name)
            assert INVALID_CREDENTIALS == response

        invalid_urn = "FAKE AS I WANNA BE"
        catalogued_id = self._identifier()
        unaffected_id = self._identifier()
        uncatalogued_id = self._identifier()
        self.collection.catalog_identifier(catalogued_id)
        self.collection.catalog_identifier(unaffected_id)

        other_collection = self._collection()
        other_collection.catalog_identifier(catalogued_id)
        other_collection.catalog_identifier(uncatalogued_id)

        with self.authenticated_request(
                '/?urn=%s&urn=%s' % (catalogued_id.urn, uncatalogued_id.urn),
                method='POST'
        ):
            # The uncatalogued identifier doesn't raise or return an error.
            response = self.controller.remove_items(self.collection.name)
            assert HTTP_OK == response.status_code

        # It sends two <simplified:message> tags.
        root = self.get_root(response.get_data())
        m = messages = self.get_messages(root)
        assert 2 == len(messages)

        # The catalogued Identifier has been removed.
        assert catalogued_id not in self.collection.catalog
        self.assert_message(m, catalogued_id, 200, 'Successfully removed')

        assert uncatalogued_id not in self.collection.catalog
        self.assert_message(m, uncatalogued_id, 404, 'Not in catalog')

        # It sends no <entry> tags.
        assert [] == self.XML_PARSE(root, "//atom:entry")

        # The catalogued identifier isn't in the catalog.
        assert catalogued_id not in self.collection.catalog
        # But it's still in the database.
        assert (catalogued_id == self._db.query(Identifier).filter_by(
            id=catalogued_id.id).one())

        # The catalog's other contents are not affected.
        assert unaffected_id in self.collection.catalog

        # The other catalog was not affected.
        assert catalogued_id in other_collection.catalog
        assert uncatalogued_id in other_collection.catalog

        # Try again, this time including an invalid URN.
        self.collection.catalog_identifier(catalogued_id)
        with self.authenticated_request(
                '/?urn=%s&urn=%s' % (invalid_urn, catalogued_id.urn),
                method='POST'
        ):
            response = self.controller.remove_items(self.collection.name)
            assert HTTP_OK == int(response.status_code)

        # Once again we get two <simplified:message> tags.
        root = self.get_root(response.get_data())
        m = messages = self.get_messages(root)
        assert 2 == len(messages)

        self.assert_message(m, invalid_urn, 400, 'Could not parse identifier.')
        self.assert_message(m, catalogued_id, 200, 'Successfully removed')

        # We have no <entry> tags.
        assert [] == self.XML_PARSE(root, "//atom:entry")

        # The catalogued identifier is still removed.
        assert catalogued_id not in self.collection.catalog


class TestIntegrationClientController(ControllerTest):

    def setup_method(self):
        super(TestIntegrationClientController, self).setup_method()
        self.controller = IntegrationClientController(self._db)

        self.http = DummyHTTPClient()

    def create_register_request_args(self, url, token=None):
        data = dict(url=url)
        if token:
            data['jwt'] = token
        return dict(
            method='POST',
            data=data,
            headers={ 'Content-Type' : 'application/x-www-form-urlencoded' }
        )

    def test_register_fails_without_url(self):
        # If not URL is given, a ProblemDetail is returned.
        request_args = self.create_register_request_args('')
        request_args['data'] = ''
        with self.app.test_request_context('/', method='POST'):
            response = self.controller.register()
        assert NO_AUTH_URL == response

    def test_register_fails_if_error_is_raised_fetching_document(self):
        def error_get(*args, **kwargs):
            raise RuntimeError('An OPDS Error')

        url = "https://test.org/okay/"
        request_args = self.create_register_request_args(url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=error_get)

        assert REMOTE_INTEGRATION_ERROR.uri == response.uri
        assert "Could not retrieve public key URL %s" % url == response.detail

    def test_register_fails_when_public_key_document_is_invalid(self):
        document_url = 'https://test.org/'
        mock_public_key_doc = json.loads(self.sample_data('public_key_document.json'))

        def assert_invalid_key_document(response, message=None):
            assert True == isinstance(response, ProblemDetail)
            assert 400 == response.status_code
            assert 'Invalid integration document' == str(response.title)
            assert response.uri.endswith('/invalid-integration-document')
            if message:
                assert message in response.detail

        def mock_response(content_json, status_code=200):
            content = json.dumps(content_json)
            headers = { 'Content-Type' : 'application/opds+json' }
            return MockRequestsResponse(
                status_code, headers=headers, content=content
            )

        # A ProblemDetail is returned when there is no public key document.
        self.http.responses.append(
            MockRequestsResponse(200, content='hi there')
        )
        request_args = self.create_register_request_args(document_url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(
            response, "Not an integration document: hi there"
        )

        # A ProblemDetail is returned when the public key document doesn't
        # have an id.
        no_id_doc = mock_public_key_doc.copy()
        del no_id_doc['id']
        self.http.responses.append(mock_response(no_id_doc))

        request_args = self.create_register_request_args(document_url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(response, 'is missing an id')

        # A ProblemDetail is returned when the public key document id
        # doesn't match the submitted OPDS url.
        self.http.responses.append(mock_response(mock_public_key_doc))
        url = 'https://fake.opds/'

        request_args = self.create_register_request_args(url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(response, "doesn't match submitted url")

        # A ProblemDetail is returned when the public key document doesn't
        # have an RSA public key.
        no_key_json = mock_public_key_doc.copy()
        del no_key_json['public_key']
        self.http.responses.append(mock_response(no_key_json))

        request_args = self.create_register_request_args(document_url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(response, "missing an RSA public_key")

        # There's a key, but the type isn't RSA.
        no_key_json['public_key'] = dict(type='safe', value='value')
        self.http.responses.append(mock_response(no_key_json))

        request_args = self.create_register_request_args(document_url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(response, "missing an RSA public_key")

        # There's an RSA public_key property, but there's no value there.
        no_key_json['public_key']['type'] = 'RSA'
        del no_key_json['public_key']['value']
        self.http.responses.append(mock_response(no_key_json))

        request_args = self.create_register_request_args(document_url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert_invalid_key_document(response, "missing an RSA public_key")

    def test_register_succeeds_with_valid_public_key_document(self):
        # Create an encryptor so we can compare secrets later. :3
        key = RSA.generate(1024)
        encryptor = PKCS1_OAEP.new(key)

        # Put the new key in the mock catalog.
        mock_auth_json = json.loads(self.sample_data('public_key_document.json'))
        mock_auth_json['public_key']['value'] = key.exportKey().decode("utf8")
        mock_public_key_doc = json.dumps(mock_auth_json)
        mock_doc_response = MockRequestsResponse(
            200, content=mock_public_key_doc,
            headers={ 'Content-Type' : 'application/opds+json' }
        )
        self.http.responses.append(mock_doc_response)

        url = 'https://test.org/'
        request_args = self.create_register_request_args(url)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)

        # An IntegrationClient has been created for this website.
        assert 201 == response.status_code
        client_qu = self._db.query(IntegrationClient).filter(
            IntegrationClient.url == 'test.org'
        )
        client = client_qu.one()

        # The appropriate login details are in the response.
        catalog = json.loads(response.data)
        assert url == catalog.get('id')
        shared_secret = catalog.get('metadata').get('shared_secret')
        decrypted_secret = encryptor.decrypt(
            stdlib_base64.b64decode(shared_secret)
        ).decode("utf8")
        assert client.shared_secret == decrypted_secret

        # If the client already has a shared_secret, a request cannot
        # succeed without providing it.
        self.http.responses.append(mock_doc_response)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
            assert INVALID_CREDENTIALS.uri == response.uri
            assert ('Cannot update existing IntegratedClient without valid shared_secret' ==
                response.detail)

        # If the existing shared secret is provided, the shared_secret
        # is updated.
        client.shared_secret = 'token'
        bearer_token = 'Bearer '+base64.b64encode('token')
        request_args['headers']['Authorization'] = bearer_token

        self.http.responses.append(mock_doc_response)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)

        assert 200 == response.status_code
        catalog = json.loads(response.data)
        # There's still only one IntegrationClient with this URL.
        client = client_qu.one()
        # It has a new shared_secret.
        assert client.shared_secret != 'token'
        shared_secret = catalog.get('metadata').get('shared_secret')
        decrypted_secret = encryptor.decrypt(
            stdlib_base64.b64decode(shared_secret)
        ).decode("utf8")
        assert client.shared_secret == decrypted_secret

    def test_register_with_jwt(self):
        # Create an encryptor so we can compare secrets later. :3
        key = RSA.generate(1024)

        # Export the keys to strings so that the jwt library can use them.
        public_key = key.publickey().exportKey()
        private_key = key.exportKey()

        encryptor = PKCS1_OAEP.new(key)
        signer = PKCS1_v1_5.new(key)

        # Use the private key to sign a JWT proving ownership of
        # the test.org web server.
        in_five_seconds = utc_now() + timedelta(seconds=5)
        payload = {'exp': in_five_seconds}
        token = jwt.encode(payload, private_key, algorithm='RS256')

        # Put the public key in the mock catalog.
        mock_auth_json = json.loads(self.sample_data('public_key_document.json'))
        mock_auth_json['public_key']['value'] = key.publickey().exportKey().decode("utf8")
        mock_public_key_doc = json.dumps(mock_auth_json)
        mock_doc_response = MockRequestsResponse(
            200, content=mock_public_key_doc,
            headers={ 'Content-Type' : 'application/opds+json' }
        )
        self.http.responses.append(mock_doc_response)

        # Send a request that includes the URL to the mock catalog,
        # and a token that proves ownership of it.
        url = 'https://test.org/'
        request_args = self.create_register_request_args(url, token)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)

        # An IntegrationClient has been created for test.org.
        assert 201 == response.status_code
        client_qu = self._db.query(IntegrationClient).filter(
            IntegrationClient.url == 'test.org'
        )
        client = client_qu.one()

        # The IntegrationClient's shared secret is in the response,
        # encrypted with the public key provided.
        catalog = json.loads(response.data)
        assert url == catalog.get('id')
        shared_secret = catalog.get('metadata').get('shared_secret')
        decrypted_secret = encryptor.decrypt(
            stdlib_base64.b64decode(shared_secret)
        ).decode("utf8")
        assert client.shared_secret == decrypted_secret

        # Since a JWT always proves ownership of test.org, we allow
        # clients who provide a JWT to modify an existing shared
        # secret without providing the old secret.
        old_secret = client.shared_secret
        self.http.responses.append(mock_doc_response)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
        assert client.shared_secret != old_secret

        # If the client provides an invalid JWT, nothing happens.
        self.http.responses.append(mock_doc_response)
        request_args = self.create_register_request_args(url, "bad token")
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
            assert INVALID_CREDENTIALS.uri == response.uri
            assert "Error decoding JWT: Not enough segments" == response.detail

        # Same if the client provides a valid but expired JWT.
        five_seconds_ago = utc_now() - timedelta(seconds=5)
        payload = {'exp': five_seconds_ago}
        token = jwt.encode(payload, private_key, algorithm='RS256')
        self.http.responses.append(mock_doc_response)
        request_args = self.create_register_request_args(url, token)
        with self.app.test_request_context('/', **request_args):
            response = self.controller.register(do_get=self.http.do_get)
            assert INVALID_CREDENTIALS.uri == response.uri
            assert "Error decoding JWT: Signature has expired" == response.detail


class TestURNLookupHandler(DatabaseTest):
    def test_process_identifier(self):
        class MockHandler(URNLookupHandler):
            ready_work = None
            def presentation_ready_work_for(self, identifier):
                return self.ready_work

            def add_status_message(self, urn, identifier):
                self.status_message = (urn, identifier)

        # If a work is already presentation-ready, it is used immediately.
        # No other code runs.
        handler = MockHandler(self._db, object(), object())
        work = object()
        handler.ready_work = work
        identifier = object()
        urn = object()
        handler.process_identifier(identifier, urn)
        assert [(identifier, work)] == handler.works

        # If a work is not presentation-ready, but calling
        # resolver.ensure_coverage makes it presentation ready, it is
        # used immediately.
        class SuccessfulResolver(object):
            force = False
            def ensure_coverage(self, identifier, force):
                handler.ready_work = work

        handler = MockHandler(self._db, SuccessfulResolver(), object())
        handler.ready_work = None
        handler.works = []
        handler.process_identifier(identifier, urn)
        assert [(identifier, work)] == handler.works

        # If a work is not presentation-ready, and calling
        # resolver.ensure_coverage does not make it presentation
        # ready, handler.add_status_message is called
        class UnsuccessfulResolver(object):
            force = False
            def ensure_coverage(self, identifier, force):
                handler.ready_work = None

        handler = MockHandler(self._db, UnsuccessfulResolver(), object())
        handler.ready_work = None
        handler.works = []
        handler.process_identifier(identifier, urn)
        assert [] == handler.works
        assert (urn, identifier) == handler.status_message

class TestURNLookupController(ControllerTest):

    ISBN_URN = 'urn:isbn:9781449358068'

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files")

    def data_file(self, path):
        """Return the contents of a test data file."""
        return open(os.path.join(self.resource_path, path), 'rb').read()

    def setup_method(self):
        super(TestURNLookupController, self).setup_method()

        self.mirror = MockS3Uploader()
        self.content_cafe = MockContentCafeAPI()

        self.overdrive_collection = MockOverdriveAPI.mock_collection(self._db)
        self.overdrive = MockOverdriveAPI(self._db, self.overdrive_collection)
        self.overdrive.queue_collection_token()

        self.viaf = MockVIAFClient(self._db)

        self.http = DummyHTTPClient()

        # The IdentifierResolutionCoverageProvider is going to instantiate
        # a number of other CoverageProviders. When it does, we want each
        # one to be instantiated with an appropriate mock API.
        individual_provider_kwargs = {
            OverdriveBibliographicCoverageProvider : dict(
                api_class=self.overdrive
            ),
            ContentCafeCoverageProvider : dict(
                api=self.content_cafe
            )
        }

        identifier_resolution_coverage_provider_kwargs = dict(
            mirror=self.mirror,
            http_get=self.http.do_get,
            viaf=self.viaf,
            provider_kwargs=individual_provider_kwargs
        )

        self.controller = URNLookupController(
            self._db,
            coverage_provider_kwargs=identifier_resolution_coverage_provider_kwargs
        )
        self.source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)

    def authenticated_request_context_resolve_now(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            from app import app

            secret = args[0].client.shared_secret.encode('utf8')
            valid_auth = 'Bearer '+ base64.urlsafe_b64encode(secret)
            headers = { 'Authorization' : valid_auth }
            with app.test_request_context('/?resolve_now=True', headers=headers):
                return f(*args, **kwargs)
        return decorated

    def one_message(self, urn, status_code, message_prefix, handler):
        """Assert that a <message> with the given status code and URN is the
        only thing in the feed.

        Return the string associated with the message, which can be
        used in further assertions.
        """
        [obj] = handler.precomposed_entries
        assert isinstance(obj, OPDSMessage)
        assert urn == obj.urn
        assert status_code == obj.status_code
        assert [] == handler.works
        if message_prefix:
            assert obj.message.startswith(message_prefix)
        return obj.message

    @authenticated_request_context
    def test_process_urn_registration_success_overdrive(self):
        # Create an Overdrive URN, then modify it to one that doesn't
        # actually exist in any database session.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID, foreign_id='changeme'
        )
        urn = identifier.urn.replace('changeme', 'abc-123-xyz')
        name = self.overdrive_collection.metadata_identifier

        handler = self.controller.process_urns([urn], metadata_identifier=name)
        message = self.one_message(
            urn, 202,
            URNLookupHandler.WORKING_TO_RESOLVE_IDENTIFIER, handler
        )

        # The Identifier was successfully resolved, that is, it was
        # registered with the CoverageProvider for Overdrive.
        assert 'operation="resolve-identifier" status=success' in message
        assert 'Overdrive - status=registered' in message

        # It was not registered with the CoverageProvider for Content Cafe,
        # since Content Cafe can't handle Overdrive IDs.
        assert 'Content Cafe' not in message

        # The Identifier has been added to the collection to await registration
        collection = self._db.query(Collection).filter(Collection.name==name).one()
        identifier = Identifier.parse_urn(self._db, urn)[0]
        assert identifier in collection.catalog

        # The CoverageRecords exist on the Identifier -- it's not
        # something that was made up for the OPDS message.
        [overdrive_cr, resolver_cr] = sorted(
            identifier.coverage_records, key=lambda x: x.operation or ""
        )
        assert DataSource.INTERNAL_PROCESSING == resolver_cr.data_source.name
        assert CoverageRecord.RESOLVE_IDENTIFIER_OPERATION == resolver_cr.operation
        assert CoverageRecord.SUCCESS == resolver_cr.status

        assert DataSource.OVERDRIVE == overdrive_cr.data_source.name
        assert None == overdrive_cr.operation
        assert CoverageRecord.REGISTERED == overdrive_cr.status

        # A LicensePool has been associated with the Identifier.
        [lp] = identifier.licensed_through

        # However, there is no Work associated with the Identifier, because
        # no CoverageProvider has run that would create one.
        assert None == identifier.work

        # Processing the URN a second time will give the same result.
        self.controller.precomposed_entries = []
        handler = self.controller.process_urns([urn], metadata_identifier=name)
        message = self.one_message(
            urn, 202,
            URNLookupHandler.WORKING_TO_RESOLVE_IDENTIFIER, handler
        )
        assert 'operation="resolve-identifier" status=success' in message
        assert 'Overdrive - status=registered' in message

    @authenticated_request_context
    def test_process_urn_registration_success_isbn(self):
        # Register an ISBN with an Overdrive collection.
        urn = self.ISBN_URN
        name = self.overdrive_collection.metadata_identifier

        handler = self.controller.process_urns([urn], metadata_identifier=name)
        message = self.one_message(
            urn, 202,
            URNLookupHandler.WORKING_TO_RESOLVE_IDENTIFIER, handler
        )

        # The Identifier was not registered with the CoverageProvider
        # for Overdrive, even though it's cataloged in an Overdrive
        # collection, because it's not of a type the Overdrive coverage
        # provider can handle.
        assert 'operation="resolve-identifier" status=success' in message
        assert 'Overdrive' not in message

        # However, it was registered with the CoverageProvider for
        # Content Cafe, which can handle an ISBN from any type of
        # collection.
        assert 'Content Cafe - status=registered' in message

    @authenticated_request_context_resolve_now
    def test_process_urn_immediate_resolution_success(self):
        # A start-to-finish test showing immediate and complete
        # resolution of an Overdrive identifier from within
        # this controller.

        # Create an Overdrive URN, then modify it to the one the mock
        # Overdrive API will expect, without actually inserting the
        # corresponding Identifier into the database.
        identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID, foreign_id='changeme'
        )
        urn = identifier.urn.replace(
            'changeme', '3896665d-9d81-4cac-bd43-ffc5066de1f5'
        )
        name = self.overdrive_collection.metadata_identifier

        # Since we are resolving everything immediately,
        # prepare the mock Overdrive API with some data.
        metadata = self.data_file("overdrive/overdrive_metadata.json")
        self.overdrive.queue_response(200, content=metadata)

        cover = self.data_file("covers/test-book-cover.png")
        self.http.queue_response(200, "image/jpeg", content=cover)
        handler = self.controller.process_urns([urn], metadata_identifier=name)

        # A presentation-ready work with a LicensePool was immediately
        # created.
        [(identifier, work)] = handler.works
        [lp] = work.license_pools
        assert urn == lp.identifier.urn
        assert DataSource.INTERNAL_PROCESSING == lp.data_source.name
        assert "Agile Documentation" == work.title

        # After processing the Overdrive data, we asked VIAF to
        # improve the author information.
        [(sort_name, display_name, known_titles)] = self.viaf.name_lookups

        # TODO: However, the data was partly used, and partly ignored
        # because the author names were too dissimilar.
        #
        # This indicates that the VIAF client needs work.
        # [contributor] = work.presentation_edition.contributors
        # assert u"Andreas R&#252;ping", contributor.display_name)
        # assert u"Kaling, Mindy", contributor.sort_name)

        # We 'downloaded' a cover image, thumbnailed it, and 'uploaded'
        # cover and thumbnail to S3.
        assert 1 == len(self.http.requests)
        assert 2 == len(self.mirror.uploaded)

    @authenticated_request_context
    def test_process_urn_registration_failure(self):
        # There are limits on how many URNs you can register at once, even if
        # you authenticate, but the limit depends on whether you
        # specified a collection.
        urn = self._identifier(identifier_type=Identifier.ISBN).urn
        name = self.overdrive_collection.metadata_identifier

        # Failure -- we didn't specify a collection.
        result = self.controller.process_urns([urn])
        assert INVALID_INPUT.uri == result.uri
        assert "No metadata identifier provided." == result.detail

        # Failure - we sent too many URNs.
        result = self.controller.process_urns([urn] * 31, metadata_identifier=name)
        assert INVALID_INPUT.uri == result.uri
        assert ("The maximum number of URNs you can provide at once is 30. (You sent 31)" ==
            result.detail)

    @authenticated_request_context_resolve_now
    def test_process_urn_registration_failure_resolving_too_much(self):
        # Even when you authenticate, you can only ask that one identifier at
        # a time be immediately resolved.
        urn = self._identifier(identifier_type=Identifier.ISBN).urn
        name = self.overdrive_collection.metadata_identifier
        result = self.controller.process_urns([urn] * 2, metadata_identifier=name)
        assert INVALID_INPUT.uri == result.uri
        assert ("The maximum number of URNs you can provide at once is 1. (You sent 2)" ==
            result.detail)

    @unauthenticated_request_context
    def test_process_urn_default_collection(self):
        # It's possible to look up individual URNs anonymously.
        urn = self.ISBN_URN

        # Test success.
        handler = self.controller.process_urns([urn])
        message = self.one_message(
            urn, 202, URNLookupHandler.WORKING_TO_RESOLVE_IDENTIFIER, handler
        )

        # The ISBN was registered with the Content Cafe coverage provider,
        # which can handle ISBNs.
        assert "Content Cafe - status=registered" in message

        # The Identifier has been added to the collection to await
        # processing.
        identifier = Identifier.parse_urn(self._db, urn)[0]
        assert identifier in self.controller.default_collection.catalog

        # When we try to register the URN with a specific collection,
        # but we're not authenticated, the Identifier is  put into the
        # unaffiliated collection instead.
        remote_collection = self._collection(external_account_id='banana')
        name = remote_collection.metadata_identifier
        urn2 = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/nosuchidentifier2"
        identifier2 = Identifier.parse_urn(self._db, urn)[0]
        self.controller.process_urns([urn2], metadata_identifier=name)
        assert identifier2 in self.controller.default_collection.catalog

        # Failure -- we sent more than one URN with an unauthenticated request.
        result = self.controller.process_urns([urn, urn2])
        assert INVALID_INPUT.uri == result.uri
        assert ("The maximum number of URNs you can provide at once is 1. (You sent 2)" ==
            result.detail)

    @unauthenticated_request_context
    def test_process_urns_unresolvable_type(self):
        # We won't even parse a Bibliotheca identifier because we
        # know we can't resolve it.
        identifier = self._identifier(Identifier.BIBLIOTHECA_ID)
        response = self.controller.process_urns([identifier.urn])
        [message] = response.precomposed_entries
        assert "Could not parse identifier." == message.message


class TestCanonicalizationController(ControllerTest):

    def setup_method(self):
        super(TestCanonicalizationController, self).setup_method()
        self.canonicalizer = SimpleMockAuthorNameCanonicalizer()
        self.controller = CanonicalizationController(
            self._db, self.canonicalizer
        )

    def test_constructor(self):
        # The default CanonicalizationController (which we don't use
        # except in this test) creates a real AuthorNameCanonicalizer
        # that's ready to make requests against real APIs.
        controller = CanonicalizationController(self._db)
        assert isinstance(controller.canonicalizer, AuthorNameCanonicalizer)

    def test_parse_identifier(self):
        # Test our slight specialization of Identifier.parse_urn.
        m = self.controller.parse_identifier
        assert None == m(None)

        # parse_urn will raise a ValueError here, but we just return None.
        assert None == m('urn:isbn:DBKACX0122823')

        isbn = m('urn:isbn:9780743593601')
        assert "9780743593601" == isbn.identifier
        assert Identifier.ISBN == isbn.type

    def test_canonicalize_author_name(self):
        m = self.controller.canonicalize_author_name

        # Test the success case.

        # First, set up a predefined right answer for an
        # Identifier/display name pair.
        identifier = self._identifier()
        input_name = "Bell Hooks"
        output_name = "hooks, bell"
        self.canonicalizer.register(input_name, identifier, output_name)

        with self.app.test_request_context(
                '/?urn=%s&display_name=%s' % (identifier.urn, input_name)
        ):
            response = self.controller.canonicalize_author_name()

            # The mock canonicalizer was asked about this
            # Identifier/name pair.
            call = self.canonicalizer.canonicalize_author_name_calls.pop()
            assert (input_name, identifier) == call

            # And it returned the predefined right answer.
            assert 200 == response.status_code
            assert "text/plain" == response.headers['Content-Type']
            assert output_name == response.data.decode("utf8")

        # Now test the failure case.
        with self.app.test_request_context('/?urn=error&display_name=nobody'):
            response = self.controller.canonicalize_author_name()

            call = self.canonicalizer.canonicalize_author_name_calls.pop()
            # We were not able to turn 'error' into a Identifier, so
            # None was passed in instead.
            assert ("nobody", None) == call

            # Since there was no predefined right answer for ("nobody", None),
            # we get a 404 error.
            assert 404 == response.status_code
            assert "" == response.data.decode("utf8")
