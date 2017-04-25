from nose.tools import (
    eq_,
    set_trace,
)
import os

from . import DatabaseTest

from core.model import (
    Collection,
    CoverageRecord, 
    DataSource,
    get_one, 
    Identifier,
    LicensePool,
)
from core.coverage import CoverageFailure
from core.opds_import import (
    MockSimplifiedOPDSLookup,
    OPDSImporter,
)
from core.s3 import DummyS3Uploader

from content_server import LookupClientCoverageProvider
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider, 
)
from oclc_classify import (
    OCLCClassifyCoverageProvider, 
)
from oclc import (
    LinkedDataCoverageProvider,
    MockOCLCLinkedData,
)

from coverage import IdentifierResolutionCoverageProvider
from oclc import LinkedDataCoverageProvider
from viaf import MockVIAFClient
from core.overdrive import (
    MockOverdriveAPI,
    OverdriveBibliographicCoverageProvider,
)

from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    BrokenCoverageProvider,
)


class MockLookupClientCoverageProvider(LookupClientCoverageProvider):

    def _lookup_client(self, url):
        return MockSimplifiedOPDSLookup(url)

    def _importer(self):
        # Part of this test is verifying that we just import the OPDS
        # metadata and don't try to make any other HTTP requests or
        # mirror anything. If we should try to do that, we'll get a 
        # crash because object() isn't really an HTTP client.
        return OPDSImporter(
            self._db, collection=self.collection,
            mirror=None, http_get=object()
        )


class TestLookupClientCoverageProvider(DatabaseTest):
    
    def setup(self):
        super(TestLookupClientCoverageProvider, self).setup()

        # Make the default collection look like a collection that goes
        # against the Library Simplified open-access content server.
        self._default_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.OA_CONTENT_SERVER
        )
        self.provider = MockLookupClientCoverageProvider(
            self._default_collection
        )
        self.lookup_client = self.provider.lookup_client
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")

    def sample_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        return open(path).read()

    def test_success(self):
        data = self.sample_data("content_server_lookup.opds")
        self.lookup_client.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Make the Identifier match the book the queued-up response is
        # talking about
        identifier.identifier = "20201"
        success = self.provider.process_item(identifier)
        eq_(success, identifier)

        # The book was imported and turned into a Work.
        [lp] = identifier.licensed_through
        work = lp.work
        eq_("Mary Gray", work.title)

        # It's not presentation-ready yet, because we are the metadata
        # wrangler and our work is not yet done.
        eq_(False, work.presentation_ready)

    def test_no_such_work(self):
        data = self.sample_data("no_such_work.opds")
        self.lookup_client.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Make the Identifier match the book the queued-up response is
        # talking about
        identifier.identifier = "2020110"
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("404: I've never heard of this work.", failure.exception)
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)

        # Most of the time this is a persistent error but it's
        # possible that we know about a book the content server
        # doesn't know about yet.
        eq_(True, failure.transient)

    def test_wrong_work_in_response(self):
        data = self.sample_data("content_server_lookup.opds")
        self.lookup_client.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # The content server told us about a different book than the
        # one we asked about.
        identifier.identifier = "999"
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_('Identifier was not mentioned in lookup response', failure.exception)
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)
        eq_(True, failure.transient)

    def test_content_server_http_failure(self):
        """Test that HTTP-level failures of the content server
        become transient CoverageFailures.
        """
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        self.lookup_client.queue_response(
            500, content="help me!"
        )
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("Got status code 500 from external server, cannot continue.",
            failure.exception)
        eq_(True, failure.transient)

        self.lookup_client.queue_response(
            200, {"content-type": "text/plain"}, content="help me!"
        )
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("OPDS Server served unhandleable media type: text/plain",
            failure.exception)
        eq_(True, failure.transient)
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)


class MockIdentifierResolutionCoverageProvider(IdentifierResolutionCoverageProvider):
    """An IdentifierResolutionCoverageProvider that makes it easy to
    plug in different required and optional CoverageProviders.
    """
    def __init__(self, *args, **kwargs):
        self.required_coverage_providers = []
        self.optional_coverage_providers = []
        super(MockIdentifierResolutionCoverageProvider, self).__init__(
            *args, **kwargs
        )
    
    def providers(self, *args, **kwargs):
        return self.required_coverage_providers, self.optional_coverage_providers
        
    
class TestIdentifierResolutionCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestIdentifierResolutionCoverageProvider, self).setup()
        self.identifier = self._identifier(Identifier.OVERDRIVE_ID)
        self._default_collection.catalog_identifier(self._db, self.identifier)
        self.source = DataSource.license_source_for(self._db, self.identifier)

        # Create mocks for the different APIs used by
        # IdentifierResolutionCoverageProvider.
        self.viaf = MockVIAFClient(self._db)
        self.linked_data_client = MockOCLCLinkedData(self._db)
        self.linked_data_coverage_provider = LinkedDataCoverageProvider(
            self._db, None, self.viaf, api=self.linked_data_client
        )
        self.uploader = DummyS3Uploader()

        # Make the constructor arguments available in case a test
        # needs to create a different type of resolver.
        self.provider_kwargs = dict(
            uploader=self.uploader,
            viaf_client=self.viaf,
            linked_data_coverage_provider=self.linked_data_coverage_provider,
        )

        # But most tests will use this resolver.
        self.resolver = MockIdentifierResolutionCoverageProvider(
            self._default_collection, **self.provider_kwargs
        )

        # Create some useful CoverageProviders that can be inserted
        # into self.resolver.required_coverage_providers
        # and self.resolver.optional_coverage_providers
        self.always_successful = AlwaysSuccessfulCoverageProvider(self._db)
        self.never_successful = NeverSuccessfulCoverageProvider(self._db)
        self.broken = BrokenCoverageProvider(self._db)

    def test_providers_opds(self):
        # For an OPDS collection that goes against the open-access content
        # server...
        self._default_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.OA_CONTENT_SERVER
        )
        uploader = object()
        # In lieu of a proper mock API, create one that will crash
        # if it tries to make a real HTTP request.
        mock_content_cafe = ContentCafeAPI(
            self._db, None, object(), object(), self.uploader
        )
        resolver = IdentifierResolutionCoverageProvider(
            self._default_collection, content_cafe_api=mock_content_cafe,
            uploader=uploader
        )

        # We get three required coverage providers: Content Cafe, OCLC
        # Classify, and OPDS Lookup Protocol.
        optional, [content_cafe, oclc_classify, opds] = resolver.providers()
        eq_([], optional)
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(oclc_classify, OCLCClassifyCoverageProvider)
        assert isinstance(opds, LookupClientCoverageProvider)
        eq_(mock_content_cafe, content_cafe.content_cafe)
        eq_(self._default_collection, opds.collection)
        
    def test_providers_overdrive(self):
        # For an Overdrive collection...
        collection = MockOverdriveAPI.mock_collection(self._db)

        # In lieu of a proper mock API, create one that will crash
        # if it tries to make a real HTTP request.
        mock_content_cafe = ContentCafeAPI(
            self._db, None, object(), object(), self.uploader
        )
        resolver = IdentifierResolutionCoverageProvider(
            collection, overdrive_api_class=MockOverdriveAPI,
            content_cafe_api=mock_content_cafe,
            uploader=self.uploader
        )

        # We get three required coverage providers: Content Cafe, OCLC
        # Classify, and Overdrive.
        optional, [content_cafe, oclc_classify, overdrive] = resolver.providers()
        eq_([], optional)
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(oclc_classify, OCLCClassifyCoverageProvider)
        assert isinstance(overdrive, OverdriveBibliographicCoverageProvider)
        
    def test_items_that_need_coverage(self):
        # Only items with an existing transient failure status require coverage.
        self._coverage_record(
            self.identifier, self.resolver.data_source,
            operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # Identifiers without coverage will be ignored.
        no_coverage = self._identifier(identifier_type=Identifier.ISBN)
        self._default_collection.catalog_identifier(self._db, no_coverage)

        # Identifiers with successful coverage will also be ignored.
        success = self._identifier(identifier_type=Identifier.ISBN)
        self._default_collection.catalog_identifier(self._db, success)
        self._coverage_record(
            success, self.resolver.data_source,
            operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION,
            status=CoverageRecord.SUCCESS
        )

        items = self.resolver.items_that_need_coverage().all()
        eq_([self.identifier], items)

    def test_process_item_creates_license_pool(self):
        self.resolver.required_coverage_providers = [
            self.always_successful
        ]

        self.resolver.process_item(self.identifier)
        [lp] = self.identifier.licensed_through
        eq_(True, isinstance(lp, LicensePool))
        eq_(lp.collection, self.resolver.collection)
        eq_(lp.data_source, self.resolver.data_source)

        # There is no Work because we don't have enough metadata
        # for this book to create one.
        eq_(None, lp.work)

    def test_process_item_may_create_work(self):
        self.resolver.required_coverage_providers = [
            self.always_successful
        ]
        edition = self._edition(
            identifier_type=self.identifier.type,
            identifier_id=self.identifier.identifier,
            authors=['Mindy K']
        )
        
        self.resolver.process_item(self.identifier)
        [lp] = self.identifier.licensed_through
        eq_(True, isinstance(lp, LicensePool))
        eq_(lp.collection, self.resolver.collection)
        eq_(lp.data_source, self.resolver.data_source)

        # Because this book had a presentation Edition, we were able
        # to create a Work.
        eq_(edition.title, lp.work.title)

        # VIAF improved the name of the author.
        eq_("Mindy Kaling", lp.work.author)

    def test_run_through_relevant_providers(self):
        providers = [self.always_successful, self.never_successful]

        # Try to process an identifier through two providers, one of
        # which will fail.
        success = self.resolver.run_through_relevant_providers(
            self.identifier, providers, fail_on_any_failure=False
        )
        # Even though one of the providers failed, the operation as a
        # whole succeeded.
        eq_(None, success)

        # Try again, under less permissive rules.
        failure = self.resolver.run_through_relevant_providers(
            self.identifier, providers, fail_on_any_failure=True
        )
        # We get a CoverageFailure representing the first failed
        # coverage provider.
        assert isinstance(failure, CoverageFailure)
        eq_("500: What did you expect?", failure.exception)
        
    def test_process_item_succeeds_if_all_required_coverage_providers_succeed(self):
        self.resolver.required_coverage_providers = [
            self.always_successful, self.always_successful
        ]

        # The coverage provider succeeded and returned an identifier.
        result = self.resolver.process_item(self.identifier)
        eq_(result, self.identifier)

    def test_process_item_fails_if_any_required_coverage_providers_fail(self):
        self.resolver.required_coverage_providers = [
            self.always_successful, self.never_successful
        ]
        result = self.resolver.process_item(self.identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_("500: What did you expect?", result.exception)
        eq_(False, result.transient)

        # The failure type of the IdentifierResolutionCoverageProvider
        # coverage record matches the failure type of the required provider's
        # coverage record.
        self.never_successful.transient = True
        result = self.resolver.process_item(self.identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.transient)

    def test_process_item_fails_when_required_provider_raises_exception(self):
        self.resolver.required_coverage_providers = [self.broken]
        result = self.resolver.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.transient)

    def test_process_item_fails_when_finalize_raises_exception(self):
        class FinalizeAlwaysFails(MockIdentifierResolutionCoverageProvider):
            def finalize(self, unresolved_identifier):
                raise Exception("Oh no!")

        provider = FinalizeAlwaysFails(
            self._default_collection, **self.provider_kwargs
        )
        result = provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        assert "Oh no!" in result.exception
        eq_(True, result.transient)

    def test_process_item_succeeds_when_optional_provider_fails(self):
        self.resolver.required_coverage_providers = [
            self.always_successful, self.always_successful
        ]

        self.resolver.optional_coverage_providers = [
            self.always_successful, self.never_successful
        ]

        result = self.resolver.process_item(self.identifier)

        # A successful result is achieved, even though the optional
        # coverage provider failed.
        eq_(result, self.identifier)

        # An appropriate coverage record was created to mark the failure.
        presentation_edition = DataSource.lookup(
            self._db, DataSource.PRESENTATION_EDITION
        )
        r = self._db.query(CoverageRecord).filter(
            CoverageRecord.identifier==self.identifier,
            CoverageRecord.data_source!=presentation_edition).one()
        eq_("What did you expect?", r.exception)
