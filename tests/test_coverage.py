from nose.tools import (
    eq_,
    set_trace,
)
import os

from . import (
    DatabaseTest,
    sample_data
)

from core.coverage import CoverageFailure
from core.model import (
    Collection,
    CoverageRecord, 
    DataSource,
    ExternalIntegration,
    get_one, 
    Identifier,
    LicensePool,
    Work,
)
from core.opds_import import (
    MockSimplifiedOPDSLookup,
    OPDSImporter,
)
from core.overdrive import (
    MockOverdriveAPI,
    OverdriveBibliographicCoverageProvider,
)
from core.s3 import DummyS3Uploader
from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    BrokenCoverageProvider,
)

from content_server import LookupClientCoverageProvider
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider, 
)
from coverage import (
    IdentifierResolutionCoverageProvider,
    IdentifierResolutionRegistrar,
)
from integration_client import (
    IntegrationClientCoverageProvider,
    WorkPresentationCoverageProvider,
)
from oclc_classify import (
    OCLCClassifyCoverageProvider, 
)
from viaf import MockVIAFClient


class MockLookupClientCoverageProvider(LookupClientCoverageProvider):

    def _lookup_client(self, url):
        return MockSimplifiedOPDSLookup(url)

    def _importer(self):
        # Part of this test is verifying that we just import the OPDS
        # metadata and don't try to make any other HTTP requests or
        # mirror anything. If we should try to do that, we'll get a 
        # crash because object() isn't really an HTTP client.
        return OPDSImporter(
            self._db, collection=self.collection, metadata_client=object(),
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
        self._default_collection.external_account_id = self._url
        self.provider = MockLookupClientCoverageProvider(
            self._default_collection
        )
        self.lookup_client = self.provider.lookup_client
        base_path = os.path.split(__file__)[0]

    def sample_data(self, filename):
        return sample_data(filename, 'opds')

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
        self._default_collection.catalog_identifier(self.identifier)
        self.source = DataSource.license_source_for(self._db, self.identifier)

        # Create mocks for the different collections and APIs used by
        # IdentifierResolutionCoverageProvider.
        self._default_collection.external_account_id = self._url
        overdrive_collection = MockOverdriveAPI.mock_collection(self._db)
        overdrive_collection.name = (
            IdentifierResolutionCoverageProvider.DEFAULT_OVERDRIVE_COLLECTION_NAME
        )
        self.viaf = MockVIAFClient(self._db)
        self.uploader = DummyS3Uploader()
        self.mock_content_cafe = ContentCafeAPI(
            self._db, None, object(), object(), self.uploader
        )

        # Make the constructor arguments available in case a test
        # needs to create a different type of resolver.
        self.provider_kwargs = dict(
            uploader=self.uploader,
            viaf_client=self.viaf,
            overdrive_api_class=MockOverdriveAPI,
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

    def test_unaffiliated_collection(self):
        """A special collection exists to track Identifiers not affiliated
        with any collection associated with a particular library.
        """
        m = IdentifierResolutionCoverageProvider.unaffiliated_collection
        unaffiliated, is_new = m(self._db)
        eq_(True, is_new)
        eq_("Unaffiliated Identifiers", unaffiliated.name)
        eq_(DataSource.INTERNAL_PROCESSING, unaffiliated.protocol)

        unaffiliated2, is_new = m(self._db)
        eq_(unaffiliated, unaffiliated2)
        eq_(False, is_new)


    def test_all(self):
        # We have 2 collections created during setup, plus 3 more
        # created here, plus the 'unaffiliated' collection.
        unaffiliated, ignore = IdentifierResolutionCoverageProvider.unaffiliated_collection(self._db)
        for i in range(3):
            collection = self._collection()

        # all() puts them in random order (not tested), but
        # the unaffiliated collection is always last.
        providers = IdentifierResolutionCoverageProvider.all(
            self._db, uploader=self.uploader, 
            content_cafe_api=self.mock_content_cafe,
        )
        eq_(6, len(providers))
        eq_(unaffiliated, providers[-1].collection)

    def test_providers_opds(self):
        # For an OPDS collection that goes against the open-access content
        # server...
        self._default_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.OA_CONTENT_SERVER
        )
        uploader = object()
        # In lieu of a proper mock API, create one that will crash
        # if it tries to make a real HTTP request.
        resolver = IdentifierResolutionCoverageProvider(
            self._default_collection, content_cafe_api=self.mock_content_cafe,
            uploader=uploader
        )

        # We get three required coverage providers: Content Cafe, OCLC
        # Classify, and OPDS Lookup Protocol.
        [content_cafe, oclc_classify, opds], optional = resolver.providers()
        eq_([], optional)
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(oclc_classify, OCLCClassifyCoverageProvider)
        assert isinstance(opds, LookupClientCoverageProvider)
        eq_(self.mock_content_cafe, content_cafe.content_cafe)
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
        [content_cafe, oclc_classify, overdrive], optional = resolver.providers()
        eq_([], optional)
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(oclc_classify, OCLCClassifyCoverageProvider)
        assert isinstance(overdrive, OverdriveBibliographicCoverageProvider)

    def test_providers_opds_for_distributors(self):
        # For an OPDS for distributors collection from a circulation manager.
        self._default_collection.protocol = ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        uploader = object()

        # In lieu of a proper mock API, create one that will crash
        # if it tries to make a real HTTP request.
        mock_content_cafe = ContentCafeAPI(
            self._db, None, object(), object(), self.uploader
        )
        resolver = IdentifierResolutionCoverageProvider(
            self._default_collection,
            content_cafe_api=mock_content_cafe,
            uploader=self.uploader
        )

        # We get one required coverage provider and two optional coverage providers.
        [integration_client], [content_cafe, oclc_classify] = resolver.providers()
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(oclc_classify, OCLCClassifyCoverageProvider)
        assert isinstance(integration_client, IntegrationClientCoverageProvider)

    def test_process_item_creates_license_pool(self):
        self.resolver.required_coverage_providers = [
            self.always_successful
        ]

        self.resolver.process_item(self.identifier)
        [lp] = self.identifier.licensed_through
        eq_(True, isinstance(lp, LicensePool))
        eq_(lp.collection, self.resolver.collection)
        eq_(lp.data_source, self.resolver.data_source)

        # Prepare an identifier that already has a LicensePool through
        # another source.
        licensed = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        lp = LicensePool.for_foreign_id(
            self._db, other_source, licensed.type, licensed.identifier,
            collection=self._default_collection
        )[0]

        self.resolver.process_item(licensed)
        eq_([lp], licensed.licensed_through)

    def test_process_item_uses_viaf_to_determine_author_name(self):
        self.resolver.required_coverage_providers = [
            self.always_successful
        ]
        edition = self._edition(
            identifier_type=self.identifier.type,
            identifier_id=self.identifier.identifier,
            authors=['Mindy K']
        )
        [original_contributor] = edition.contributors
        eq_('K, Mindy', original_contributor.sort_name)

        self.resolver.process_item(self.identifier)
        [lp] = self.identifier.licensed_through
        eq_(True, isinstance(lp, LicensePool))
        eq_(lp.collection, self.resolver.collection)
        eq_(lp.data_source, self.resolver.data_source)

        # Because this book had a presentation edition, a work was
        # generated.
        work = self.identifier.work
        assert isinstance(work, Work)
        eq_(edition, work.presentation_edition)
        eq_(edition.title, work.title)

        # VIAF updated the same contributor object with better name data.
        eq_([original_contributor], list(edition.contributors))
        eq_("Kaling, Mindy", original_contributor.sort_name)
        eq_("Mindy Kaling", original_contributor.display_name)

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
        # Give the identifier an edition so a work can be created.
        edition = self._edition(
            identifier_type=self.identifier.type,
            identifier_id=self.identifier.identifier
        )

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

    def test_process_item_succeeds_when_optional_provider_fails(self):
        # Give the identifier an edition so a work can be created.
        edition = self._edition(
            identifier_type=self.identifier.type,
            identifier_id=self.identifier.identifier
        )

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
        r = self._db.query(CoverageRecord).filter(
            CoverageRecord.identifier==self.identifier,
            CoverageRecord.operation==self.never_successful.OPERATION).one()
        eq_("What did you expect?", r.exception)

    def test_process_item_registers_work_for_calculation(self):
        # Give the identifier an edition so a work can be created.
        edition = self._edition(
            identifier_type=self.identifier.type,
            identifier_id=self.identifier.identifier
        )

        self.resolver.required_coverage_providers = [self.always_successful]
        result = self.resolver.process_item(self.identifier)

        eq_(result, self.identifier)

        # The identifier has been given a work.
        work = self.identifier.work
        assert isinstance(work, Work)

        # The work has a WorkCoverageRecord for presentation calculation.
        [record] = [r for r in work.coverage_records
                    if r.operation==WorkPresentationCoverageProvider.OPERATION]


class TestIdentifierResolutionRegistrar(DatabaseTest):

    PROVIDER = IdentifierResolutionCoverageProvider

    def setup(self):
        super(TestIdentifierResolutionRegistrar, self).setup()
        self.registrar = IdentifierResolutionRegistrar(self._default_collection)
        self.identifier = self._identifier()

    def test_resolution_coverage(self):
        # Returns None if the identifier doesn't have a coverage record
        # for the IdentifierResolutionCoverageProvider.
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        cr = self._coverage_record(self.identifier, source)
        result = self.registrar.resolution_coverage(self.identifier)
        eq_(None, result)

        # Returns an IdentifierResolutionCoverageProvider record if it exists.
        source = DataSource.lookup(self._db, self.PROVIDER.DATA_SOURCE_NAME)
        cr = self._coverage_record(
            self.identifier, source, operation=self.PROVIDER.OPERATION
        )
        result = self.registrar.resolution_coverage(self.identifier)
        eq_(cr, result)

    def test_process_item_does_not_catalog_already_cataloged_identifier(self):
        # This identifier is already in a Collection's catalog.
        collection = self._collection(data_source_name=DataSource.OA_CONTENT_SERVER)
        collection.catalog.append(self.identifier)

        # Registering it as unresolved doesn't also add it to the
        # 'unaffiliated' Collection.
        self.registrar.process_item(self.identifier)
        eq_([collection], self.identifier.collections)

    def test_process_item_catalogs_unaffiliated_identifiers(self):
        # This identifier has no collection.
        eq_(0, len(self.identifier.collections))

        unaffiliated_collection, ignore = self.PROVIDER.unaffiliated_collection(self._db)
        self.registrar.process_item(self.identifier)
        eq_([unaffiliated_collection], self.identifier.collections)

    def test_process_item_creates_expected_initial_coverage_records(self):

        def assert_initial_coverage_record(record):
            eq_(CoverageRecord.REGISTERED, record.status)
            eq_(None, record.exception)

        def assert_expected_coverage_records_created(
            records, expected_source_names
        ):
            # The expected number of CoverageRecords was created.
            eq_(len(expected_source_names), len(records))
            resulting_sources = list()
            for cr in records:
                # Each CoverageRecord has the expected error details.
                assert_initial_coverage_record(cr)
                resulting_sources.append(cr.data_source.name)
            # The CoverageRecords created are for the metadata we would expect.
            eq_(sorted(expected_source_names), sorted(resulting_sources))

        test_cases = {
            Identifier.OVERDRIVE_ID : [
                DataSource.INTERNAL_PROCESSING,
                DataSource.OCLC_LINKED_DATA,
                DataSource.OVERDRIVE,
            ],
            Identifier.ISBN : [
                DataSource.INTERNAL_PROCESSING,
                DataSource.CONTENT_CAFE,
                DataSource.OCLC_LINKED_DATA,
            ],
            Identifier.GUTENBERG_ID : [
                DataSource.INTERNAL_PROCESSING,
                DataSource.OCLC,
            ]
        }

        for identifier_type, expected_source_names in test_cases.items():
            # Confirm the identifier begins without coverage records.
            eq_(0, len(self.identifier.coverage_records))

            self.identifier.type = identifier_type
            self.registrar.process_item(self.identifier)

            assert_expected_coverage_records_created(
                self.identifier.coverage_records, expected_source_names
            )
            for cr in self.identifier.coverage_records:
                # Delete the CoverageRecords so the next tests can run.
                self._db.delete(cr)
            self._db.commit()

        # If the identifier is associated with a collection, the appropriate
        # CoverageRecords are added.
        opds_distrib = self._collection(
            protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        )
        opds_import = self._collection(data_source_name=DataSource.OA_CONTENT_SERVER)
        self.identifier.collections.extend([opds_distrib, opds_import])
        self.registrar.process_item(self.identifier)

        # A CoverageRecord is created for the OA content server.
        [oa_content] = [cr for cr in self.identifier.coverage_records
                        if cr.data_source.name==DataSource.OA_CONTENT_SERVER]
        # It doesn't have a collection, even though it used a collection
        # to provide a DataSource.
        eq_(None, oa_content.collection)

        # There should be an additional DataSource.INTERNAL_PROCESSING
        # record for the OPDS_FOR_DISTRIBUTORS coverage.
        source_names = [cr.data_source.name
                        for cr in self.identifier.coverage_records]
        eq_(2, source_names.count(DataSource.INTERNAL_PROCESSING))

    def test_process_item_creates_an_active_license_pool(self):
        # Confirm the identifier has no LicensePool.
        eq_([], self.identifier.licensed_through)

        # After registration, there's a LicensePool.
        self.registrar.process_item(self.identifier)

        [lp] = self.identifier.licensed_through
        eq_(lp.data_source.name, DataSource.INTERNAL_PROCESSING)
        eq_(1, lp.licenses_owned)
        eq_(1, lp.licenses_available)

        # If the Identifier already has a LicensePool (highly unlikely!),
        # no new LicensePool is created.
        pool = self._licensepool(None)
        identifier = pool.identifier
        pool.licenses_owned = 4
        pool.licenses_available = 2

        # Registration does not create a new LicensePool.
        self.registrar.process_item(identifier)
        [lp] = identifier.licensed_through
        eq_(pool, lp)

        # It also doesn't adjust the available licenses.
        eq_(4, lp.licenses_owned)
        eq_(2, lp.licenses_available)
