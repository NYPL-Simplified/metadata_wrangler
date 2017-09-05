import os
import base64
import feedparser
import json
import urllib
from StringIO import StringIO
from datetime import datetime, timedelta
from functools import wraps
from lxml import etree
from nose.tools import set_trace, eq_

from . import DatabaseTest
from core.model import (
    IntegrationClient,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    get_one,
)
from core.problem_details import (
    INVALID_CREDENTIALS,
    INVALID_INPUT,
)
from core.util.problem_detail import ProblemDetail
from core.util.opds_writer import OPDSMessage
from core.opds_import import OPDSXMLParser

from controller import (
    CatalogController,
    URNLookupController,
    HTTP_OK,
    HTTP_CREATED,
    HTTP_ACCEPTED,
    HTTP_UNAUTHORIZED,
    HTTP_NOT_FOUND,
    HTTP_INTERNAL_SERVER_ERROR,
    authenticated_client_from_request,
)


class ControllerTest(DatabaseTest):

    def setup(self):
        super(ControllerTest, self).setup()

        from app import app
        self.app = app

        self.client = self._integration_client()
        valid_auth = 'Basic ' + base64.b64encode('abc:def')
        self.valid_auth = dict(Authorization=valid_auth)


class TestIntegrationClientAuthentication(ControllerTest):

    def test_authenticated_client_required(self):
        # Returns catalog if authentication is valid.
        with self.app.test_request_context('/', headers=self.valid_auth):
            result = authenticated_client_from_request(self._db)
            eq_(result, self.client)
        
        # Returns error if authentication is invalid.
        invalid_auth = 'Basic ' + base64.b64encode('abc:defg')
        with self.app.test_request_context('/',
                headers=dict(Authorization=invalid_auth)):
            result = authenticated_client_from_request(self._db)
            eq_(True, isinstance(result, ProblemDetail))
            eq_(HTTP_UNAUTHORIZED, result.status_code)

        # Returns errors without authentication.
        with self.app.test_request_context('/'):
            result = authenticated_client_from_request(self._db)
            eq_(True, isinstance(result, ProblemDetail))

    def test_authenticated_client_optional(self):
        # Returns catalog of authentication is valid.
        with self.app.test_request_context('/', headers=self.valid_auth):
            result = authenticated_client_from_request(self._db, required=False)
            eq_(result, self.client)
        
        # Returns error if attempted authentication is invalid.
        invalid_auth = 'Basic ' + base64.b64encode('abc:defg')
        with self.app.test_request_context('/',
                headers=dict(Authorization=invalid_auth)):
            result = authenticated_client_from_request(self._db, required=False)
            eq_(True, isinstance(result, ProblemDetail))
            eq_(HTTP_UNAUTHORIZED, result.status_code)

        # Returns none if no authentication.
        with self.app.test_request_context('/'):
            result = authenticated_client_from_request(self._db, required=False)
            eq_(None, result)


class TestCatalogController(ControllerTest):

    XML_PARSE = OPDSXMLParser()._xpath

    def setup(self):
        super(TestCatalogController, self).setup()
        self.controller = CatalogController(self._db)

        # The collection as it exists on the circulation manager.
        remote_collection = self._collection(username='test_coll', external_account_id=self._url)
        # The collection as it is recorded / catalogued here.
        self.collection = self._collection(
            name=remote_collection.metadata_identifier,
            protocol=remote_collection.protocol
        )

        self.work1 = self._work(with_license_pool=True, with_open_access_download=True)
        self.work2 = self._work(with_license_pool=True, with_open_access_download=True)

    def xml_value(self, message, tag):
        return self.XML_PARSE(message, tag)[0].text

    def test_updates_feed(self):
        identifier = self.work1.license_pools[0].identifier
        self.collection.catalog_identifier(self._db, identifier)

        with self.app.test_request_context('/', headers=self.valid_auth):
            response = self.controller.updates_feed(self.collection.name)
            # The catalog's updates feed is returned.
            eq_(HTTP_OK, response.status_code)
            feed = feedparser.parse(response.get_data())
            eq_(feed.feed.title,
                u"%s Collection Updates for %s" % (self.collection.protocol, self.client.url))

            # The feed has the catalog's catalog.
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(self.work1.title, entry['title'])
            eq_(identifier.urn, entry['id'])

        # A time can be passed.
        time = datetime.utcnow()
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        for record in self.work1.coverage_records:
            # Set back the clock on all of work1's time records
            record.timestamp = time - timedelta(days=1)
        with self.app.test_request_context('/?last_update_time=%s' % timestamp,
            headers=self.valid_auth):
            response = self.controller.updates_feed(self.collection.name)
            eq_(HTTP_OK, response.status_code)
            feed = feedparser.parse(response.get_data())
            eq_(feed.feed.title,
                u"%s Collection Updates for %s" % (self.collection.protocol, self.client.url))

            # The timestamp is included in the url.
            linkified_timestamp = time.strftime("%Y-%m-%d+%H:%M:%S").replace(":", "%3A")
            assert feed['feed']['id'].endswith(linkified_timestamp)
            # And only works updated since the timestamp are returned.
            eq_(0, len(feed['entries']))

        # Works updated since the timestamp are returned
        self.work1.coverage_records[0].timestamp = datetime.utcnow()
        with self.app.test_request_context('/?last_update_time=%s' % timestamp,
            headers=self.valid_auth):
            response = self.controller.updates_feed(self.collection.name)
            feed = feedparser.parse(response.get_data())
            eq_(1, len(feed['entries']))
            [entry] = feed['entries']
            eq_(self.work1.title, entry['title'])
            eq_(identifier.urn, entry['id'])

    def test_updates_feed_is_paginated(self):
        for work in [self.work1, self.work2]:
            self.collection.catalog_identifier(
                self._db, work.license_pools[0].identifier
            )
        with self.app.test_request_context('/?size=1',
            headers=self.valid_auth):
            response = self.controller.updates_feed(self.collection.name)
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'next' for link in links])
            assert not any([link['rel'] == 'previous' for link in links])
            assert not any([link['rel'] == 'first' for l in links])

        with self.app.test_request_context('/?size=1&after=1',
            headers=self.valid_auth):
            response = self.controller.updates_feed(self.collection.name)
            links = feedparser.parse(response.get_data())['feed']['links']
            assert any([link['rel'] == 'previous' for link in links])
            assert any([link['rel'] == 'first' for link in links])
            assert not any([link['rel'] == 'next'for link in links])

    def test_add_items(self):
        invalid_urn = "FAKE AS I WANNA BE"
        catalogued_id = self._identifier()
        uncatalogued_id = self._identifier()
        self.collection.catalog_identifier(self._db, catalogued_id)

        parser = OPDSXMLParser()
        message_path = '/atom:feed/simplified:message'

        with self.app.test_request_context(
                '/?urn=%s&urn=%s&urn=%s' % (
                catalogued_id.urn, uncatalogued_id.urn, invalid_urn),
                headers=self.valid_auth):

            response = self.controller.add_items(self.collection.name)

        # None of the identifiers raise or return an error.
        eq_(HTTP_OK, response.status_code)

        # It sends three messages.
        root = etree.parse(StringIO(response.data))
        catalogued, uncatalogued, invalid = self.XML_PARSE(root, message_path)

        # The uncatalogued identifier is now in the catalog.
        assert uncatalogued_id in self.collection.catalog
        # It has an accurate response message.
        eq_(uncatalogued_id.urn, self.xml_value(uncatalogued, 'atom:id'))
        eq_('201', self.xml_value(uncatalogued, 'simplified:status_code'))
        eq_('Successfully added', self.xml_value(uncatalogued, 'schema:description'))

        # The catalogued identifier is still in the catalog.
        assert catalogued_id in self.collection.catalog
        # And even though it responds 'OK', the message tells you it
        # was already there.
        eq_(catalogued_id.urn, self.xml_value(catalogued, 'atom:id'))
        eq_('200', self.xml_value(catalogued, 'simplified:status_code'))
        eq_('Already in catalog', self.xml_value(catalogued, 'schema:description'))

        # Invalid identifier return 400 errors.
        eq_(invalid_urn, self.xml_value(invalid, 'atom:id'))
        eq_('400', self.xml_value(invalid, 'simplified:status_code'))
        eq_('Could not parse identifier.', self.xml_value(invalid, 'schema:description'))

    def test_add_with_metadata(self):
        # Pretend this content server OPDS came from a circulation manager.
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "opds")
        path = os.path.join(resource_path, "content_server_lookup.opds")
        opds = open(path).read()

        # And here's some OPDS with an invalid identifier.
        invalid_opds = "<feed><entry><id>invalid</id></entry></feed>"

        parser = OPDSXMLParser()
        message_path = '/atom:feed/simplified:message'

        with self.app.test_request_context(headers=self.valid_auth, data=opds):
            response = self.controller.add_with_metadata(self.collection.name)

        eq_(HTTP_OK, response.status_code)

        # It sends one message.
        root = etree.parse(StringIO(response.data))
        [catalogued] = self.XML_PARSE(root, message_path)

        # The identifier in the OPDS feed is now in the catalog.
        identifier = self._identifier(foreign_id='20201')
        assert identifier in self.collection.catalog

        # It has an accurate response message.
        eq_(identifier.urn, self.xml_value(catalogued, 'atom:id'))
        eq_('201', self.xml_value(catalogued, 'simplified:status_code'))
        eq_('Successfully added', self.xml_value(catalogued, 'schema:description'))

        # The identifier has links for the cover images from the feed.
        eq_(set(["http://s3.amazonaws.com/book-covers.nypl.org/Gutenberg%20Illustrated/20201/cover_20201_0.png",
                 "http://s3.amazonaws.com/book-covers.nypl.org/Gutenberg%20Illustrated/20201/cover_20201_0.png"]),
            set([link.resource.url for link in identifier.links]))
        eq_(set([Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]),
            set([link.rel for link in identifier.links]))

        # The identifier has a LicensePool.
        eq_(1, len(identifier.licensed_through))
        eq_(self.collection, identifier.licensed_through[0].collection)

        # The identifier also has an Edition with title, author, and language.
        edition = get_one(self._db, Edition, primary_identifier=identifier)
        eq_("Mary Gray", edition.title)
        [author] = edition.contributors
        eq_(Edition.UNKNOWN_AUTHOR, author.sort_name)
        eq_("en", edition.language)

        # Finally, the identifier has a transient failure CoverageRecord so it will
        # be processed by the identifier resolution script.
        data_source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        record = CoverageRecord.lookup(identifier, data_source,
                                        CoverageRecord.RESOLVE_IDENTIFIER_OPERATION)
        eq_(CoverageRecord.TRANSIENT_FAILURE, record.status)
        eq_(self.collection, record.collection)

        record.status = CoverageRecord.SUCCESS

        # If we make the same request again, the identifier stays in the catalog.
        with self.app.test_request_context(headers=self.valid_auth, data=opds):
            response = self.controller.add_with_metadata(self.collection.name)

        eq_(HTTP_OK, response.status_code)

        # It sends one message.
        root = etree.parse(StringIO(response.data))
        [catalogued] = self.XML_PARSE(root, message_path)

        # The identifier in the OPDS feed is still in the catalog.
        assert identifier in self.collection.catalog

        # And even though it responds 'OK', the message tells you it
        # was already there.
        eq_(identifier.urn, self.xml_value(catalogued, 'atom:id'))
        eq_('200', self.xml_value(catalogued, 'simplified:status_code'))
        eq_('Already in catalog', self.xml_value(catalogued, 'schema:description'))

        # The coverage record has been set back to transient failure since
        # there's new information to process.
        eq_(CoverageRecord.TRANSIENT_FAILURE, record.status)

        # The invalid identifier returns a 400 error message.
        with self.app.test_request_context(headers=self.valid_auth, data=invalid_opds):
            response = self.controller.add_with_metadata(self.collection.name)
        eq_(HTTP_OK, response.status_code)

        # It sends one message.
        root = etree.parse(StringIO(response.data))
        [invalid] = self.XML_PARSE(root, message_path)

        eq_("invalid", self.xml_value(invalid, 'atom:id'))
        eq_('400', self.xml_value(invalid, 'simplified:status_code'))
        eq_('Could not parse identifier.', self.xml_value(invalid, 'schema:description'))

    def test_remove_items(self):
        invalid_urn = "FAKE AS I WANNA BE"
        catalogued_id = self._identifier()
        uncatalogued_id = self._identifier()
        self.collection.catalog_identifier(self._db, catalogued_id)

        message_path = '/atom:feed/simplified:message'
        with self.app.test_request_context(
                '/?urn=%s&urn=%s' % (catalogued_id.urn, uncatalogued_id.urn),
                headers=self.valid_auth):

            # The uncatalogued identifier doesn't raise or return an error.
            response = self.controller.remove_items(self.collection.name)
            eq_(HTTP_OK, response.status_code)            

            # It sends two <simplified:message> tags.
            root = etree.parse(StringIO(response.data))
            catalogued, uncatalogued = self.XML_PARSE(root, message_path)

            eq_(catalogued_id.urn, self.xml_value(catalogued, 'atom:id'))
            eq_(str(HTTP_OK), self.xml_value(catalogued, 'simplified:status_code'))
            eq_("Successfully removed", self.xml_value(catalogued, 'schema:description'))

            eq_(uncatalogued_id.urn, self.xml_value(uncatalogued, 'atom:id'))
            eq_(str(HTTP_NOT_FOUND), self.xml_value(uncatalogued, 'simplified:status_code'))
            eq_("Not in catalog", self.xml_value(uncatalogued, 'schema:description'))

            # It sends no <entry> tags.
            eq_([], self.XML_PARSE(root, "//atom:entry"))

            # The catalogued identifier isn't in the catalog.
            assert catalogued_id not in self.collection.catalog
            # But it's still in the database.
            eq_(catalogued_id, self._db.query(Identifier).filter_by(
                id=catalogued_id.id).one())

        # Try again, this time including an invalid URN.
        self.collection.catalog_identifier(self._db, catalogued_id)
        with self.app.test_request_context(
                '/?urn=%s&urn=%s' % (invalid_urn, catalogued_id.urn),
                headers=self.valid_auth):
            response = self.controller.remove_items(self.collection.name)
            eq_(HTTP_OK, int(response.status_code))

            # Once again we get two <simplified:message> tags.
            root = etree.parse(StringIO(response.data))
            invalid, catalogued = self.XML_PARSE(root, message_path)

            eq_(invalid_urn, self.xml_value(invalid, 'atom:id'))
            eq_("400", self.xml_value(invalid, 'simplified:status_code'))
            eq_("Could not parse identifier.", self.xml_value(invalid, 'schema:description'))

            eq_(catalogued_id.urn, self.xml_value(catalogued, 'atom:id'))
            eq_("200", self.xml_value(catalogued, 'simplified:status_code'))
            eq_("Successfully removed", self.xml_value(catalogued, 'schema:description'))

            # We have no <entry> tags.
            eq_([], self.XML_PARSE(root, "//atom:entry"))
            
            # The catalogued identifier is still removed.
            assert catalogued_id not in self.collection.catalog

    def test_update_client_url(self):
        url = urllib.quote('https://try-me.fake.us/')
        with self.app.test_request_context('/'):
            # Without authentication a ProblemDetail is returned.
            response = self.controller.update_client_url()
            eq_(True, isinstance(response, ProblemDetail))
            eq_(INVALID_CREDENTIALS, response)

        with self.app.test_request_context('/', headers=self.valid_auth):
            # When a URL isn't provided, a ProblemDetail is returned.
            response = self.controller.update_client_url()
            eq_(True, isinstance(response, ProblemDetail))
            eq_(400, response.status_code)
            eq_(INVALID_INPUT.uri, response.uri)
            assert 'client_url' in response.detail

        with self.app.test_request_context('/?client_url=%s' % url,
            headers=self.valid_auth):
            response = self.controller.update_client_url()
            # The request was successful.
            eq_(HTTP_OK, response.status_code)
            # The IntegrationClient's URL has been changed.
            self.client.url = 'try-me.fake.us'


class TestURNLookupController(ControllerTest):

    def setup(self):
        super(TestURNLookupController, self).setup()
        self.controller = URNLookupController(self._db)
        self.source = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)

    def basic_request_context(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            from app import app
            with app.test_request_context('/'):
                return f(*args, **kwargs)
        return decorated

    @basic_request_context
    def assert_one_message(self, urn, code, message):
        """Assert that the given message is the only thing
        in the feed.
        """
        [obj] = self.controller.precomposed_entries
        expect = OPDSMessage(urn, code, message)
        assert isinstance(obj, OPDSMessage)
        eq_(urn, obj.urn)
        eq_(code, obj.status_code)
        eq_(message, obj.message)
        eq_([], self.controller.works)

    @basic_request_context
    def test_process_urn_initial_registration(self):
        urn = Identifier.URN_SCHEME_PREFIX + "Overdrive ID/nosuchidentifier"
        self.controller.process_urn(urn)
        self.assert_one_message(
            urn, 201, URNLookupController.IDENTIFIER_REGISTERED
        )

        # The Identifier has been created and given a CoverageRecord
        # with a transient failure.
        [identifier] = self._db.query(Identifier).filter(
            Identifier.type==Identifier.OVERDRIVE_ID
        ).all()
        eq_("nosuchidentifier", identifier.identifier)
        [coverage] = identifier.coverage_records
        eq_(CoverageRecord.TRANSIENT_FAILURE, coverage.status)

    @basic_request_context
    def test_process_urn_pending_resolve_attempt(self):
        # Simulate calling process_urn twice, and make sure the 
        # second call results in an "I'm working on it, hold your horses" message.
        identifier = self._identifier(Identifier.GUTENBERG_ID)

        record, is_new = CoverageRecord.add_for(
            identifier, self.source, self.controller.OPERATION,
            status=CoverageRecord.TRANSIENT_FAILURE
        )
        record.exception = self.controller.NO_WORK_DONE_EXCEPTION

        self.controller.process_urn(identifier.urn)
        self.assert_one_message(
            identifier.urn, HTTP_ACCEPTED,
            URNLookupController.WORKING_TO_RESOLVE_IDENTIFIER
        )

    @basic_request_context
    def test_process_urn_exception_during_resolve_attempt(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        record, is_new = CoverageRecord.add_for(
            identifier, self.source, self.controller.OPERATION,
            status=CoverageRecord.PERSISTENT_FAILURE
        )
        record.exception = "foo"
        self.controller.process_urn(identifier.urn)
        self.assert_one_message(
            identifier.urn, HTTP_INTERNAL_SERVER_ERROR, "foo"
        )

    @basic_request_context
    def test_process_urn_no_presentation_ready_work(self):
        identifier = self._identifier(Identifier.GUTENBERG_ID)

        # There's a record of success, but no presentation-ready work.
        record, is_new = CoverageRecord.add_for(
            identifier, self.source, self.controller.OPERATION,
            status=CoverageRecord.SUCCESS
        )

        self.controller.process_urn(identifier.urn)
        self.assert_one_message(
            identifier.urn, HTTP_INTERNAL_SERVER_ERROR,
            self.controller.SUCCESS_DID_NOT_RESULT_IN_PRESENTATION_READY_WORK
        )

    @basic_request_context
    def test_process_urn_unresolvable_type(self):
        # We can't resolve a 3M identifier because we don't have the
        # appropriate access to the bibliographic API.
        identifier = self._identifier(Identifier.THREEM_ID)
        self.controller.process_urn(identifier.urn)
        self.assert_one_message(
            identifier.urn, HTTP_NOT_FOUND, self.controller.UNRESOLVABLE_IDENTIFIER
        )

    @basic_request_context
    def test_presentation_ready_work_overrides_unresolveable_type(self):
        # If there is a presentation-ready Work associated
        # with the identifier, turns out we can resolve it even if the
        # type would otherwise not be resolvable.
        edition, pool = self._edition(
            identifier_type=Identifier.THREEM_ID, with_license_pool=True
        )
        pool.open_access = False
        work, is_new = pool.calculate_work()
        work.presentation_ready = True
        identifier = edition.primary_identifier
        self.controller.process_urn(identifier.urn)
        eq_([(identifier, work)], self.controller.works)

    def test_process_urn_with_collection(self):
        name = base64.b64encode((ExternalIntegration.OPDS_IMPORT+':'+self._url), '-_')
        collection = self._collection(name=name, url=self._url)

        with self.app.test_request_context('/', headers=self.valid_auth):
            i1 = self._identifier()
            i2 = self._identifier()

            eq_([], collection.catalog)
            self.controller.process_urn(i1.urn, collection_details=name)
            eq_(1, len(collection.catalog))
            eq_([i1], collection.catalog)

            # Adds new identifiers to an existing collection's catalog
            self.controller.process_urn(i2.urn, collection_details=name)
            eq_(2, len(collection.catalog))
            eq_(sorted([i1, i2]), sorted(collection.catalog))

            # Does not duplicate identifiers in the collection's catalog
            self.controller.process_urn(i1.urn, collection_details=name)
            eq_(2, len(collection.catalog))
            eq_(sorted([i1, i2]), sorted(collection.catalog))

        with self.app.test_request_context('/'):
            # Does not add identifiers to a collection if it isn't
            # sent by an authenticated client, even if there's a
            # collection attached.
            i3 = self._identifier()
            self.controller.process_urn(i3.urn, collection_details=name)
            assert i3 not in collection.catalog

    @basic_request_context
    def test_process_urn_isbn(self):
        # Create a new ISBN identifier.
        # Ask online providers for metadata to turn into an opds feed about this identifier.
        # Make sure a coverage record was created, and a 201 status obtained from provider.
        # Ask online provider again, and make sure we're now getting a 202 "working on it" status.
        # Ask again, this time getting a result.  Make sure know that got a result.

        isbn, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, self._isbn
        )

        # The first time we look up an ISBN a CoverageRecord is created
        # representing the work to be done.
        self.controller.process_urn(isbn.urn)
        self.assert_one_message(
            isbn.urn, HTTP_CREATED, self.controller.IDENTIFIER_REGISTERED
        )
        [record] = isbn.coverage_records
        eq_(record.exception, self.controller.NO_WORK_DONE_EXCEPTION)
        eq_(record.status, CoverageRecord.TRANSIENT_FAILURE)

        # So long as the necessary coverage is not provided,
        # future lookups will not provide useful information
        self.controller.precomposed_entries = []
        self.controller.process_urn(isbn.urn)
        self.assert_one_message(
            isbn.urn, HTTP_ACCEPTED, self.controller.WORKING_TO_RESOLVE_IDENTIFIER
        )

        # Let's provide the coverage.
        metadata_sources = DataSource.metadata_sources_for(
            self._db, isbn
        )
        for source in metadata_sources:
            CoverageRecord.add_for(isbn, source)

        # Process the ISBN again, and we get an <entry> tag with the
        # information.
        self.controller.precomposed_entries = []
        self.controller.process_urn(isbn.urn)
        expect = isbn.opds_entry()
        [actual] = self.controller.precomposed_entries
        eq_(etree.tostring(expect), etree.tostring(actual))

