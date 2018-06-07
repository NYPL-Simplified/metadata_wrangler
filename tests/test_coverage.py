from nose.tools import (
    eq_,
    set_trace,
)

from . import DatabaseTest

from core.model import (
    DataSource,
    ExternalIntegration,
)

from core.s3 import S3Uploader
from core.coverage import CollectionCoverageProvider
from core.overdrive import MockOverdriveAPI

from content_cafe import ContentCafeCoverageProvider
from coverage import IdentifierResolutionCoverageProvider
from integration_client import IntegrationClientCoverImageCoverageProvider
from oclc_classify import OCLCClassifyCoverageProvider 
from overdrive import OverdriveBibliographicCoverageProvider
from viaf import VIAFClient


class TestIdentifierResolutionCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestIdentifierResolutionCoverageProvider, self).setup()

        # Set up a sitewide storage integration.
        storage = self._external_integration(
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
            username="a",
            password="b"
        )

        # Set up a Content Cafe integration
        content_cafe = self._external_integration(
            goal=ExternalIntegration.METADATA_GOAL,
            protocol=ExternalIntegration.CONTENT_CAFE,
            username="a",
            password="b"
        )

    def test_constructor(self):
        """Test that the constructor does the right thing when
        given arguments like those used in production.

        Other tests will invoke the constructor with mock objects.
        """
        class Mock(IdentifierResolutionCoverageProvider):
            def gather_providers(self, provider_kwargs):
                return ["a provider"]

        immediate = object()
        force = object()
        provider = Mock(
            self._default_collection, provide_coverage_immediately=immediate,
            force=force, batch_size=93
        )

        # We aim to resolve all the identifiers in a given collection.
        eq_(self._default_collection, provider.collection)

        eq_(immediate, provider.provide_coverage_immediately)
        eq_(force, provider.force)

        # A random extra keyword argument was propagated to the
        # super-constructor.
        eq_(93, provider.batch_size)

        policy = provider.replacement_policy
        # We will be taking extra care to regenerate the OPDS entries
        # of the Works we deal with.
        eq_(
            True,
            policy.presentation_calculation_policy.regenerate_opds_entries
        )

        # An S3Uploader was instantiated from our storage integration.
        assert isinstance(policy.mirror, S3Uploader)

        # A VIAFClient was instantiated as well.
        assert isinstance(provider.viaf, VIAFClient)

        # The sub-providers were set up by calling gather_providers().
        eq_(["a provider"], provider.providers)

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
        class Mock(IdentifierResolutionCoverageProvider):
            def gather_providers(self, provider_kwargs):
                return []

        # We have 3 collections created here, plus the 'unaffiliated'
        # collection.
        unaffiliated, ignore = IdentifierResolutionCoverageProvider.unaffiliated_collection(self._db)
        for i in range(3):
            collection = self._collection()

        # all() puts them in random order (not tested), but
        # the unaffiliated collection is always last.
        providers = Mock.all(self._db, mirror=object())
        providers = list(providers)
        eq_(4, len(providers))
        eq_(unaffiliated, providers[-1].collection)

    def test_gather_providers_overdrive(self):
        overdrive_collection = MockOverdriveAPI.mock_collection(self._db)
        provider_kwargs = {
            OverdriveBibliographicCoverageProvider : dict(
                api_class=MockOverdriveAPI
            )
        }
        provider = IdentifierResolutionCoverageProvider(
            overdrive_collection, provider_kwargs=provider_kwargs
        )

        # We got two CoverageProviders -- one that can handle a collection
        # based on any protocol, and one that can only handle a collection
        # based on the Overdrive protocol.
        content_cafe, overdrive = provider.providers
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(overdrive, OverdriveBibliographicCoverageProvider)

        # The MockOverdriveAPI we passed in as part of provider_kwargs
        # was instantiated as part of the
        # OverdriveBibliographicCoverageProvider instantiation.
        assert isinstance(overdrive.api, MockOverdriveAPI)

        # Since the Overdrive coverage provider checks with VIAF after
        # adding author information to the database, it's been given a
        # reference to our VIAF client.
        eq_(provider.viaf, overdrive.viaf)

        # All subproviders are associated with the collection used in the
        # main provider, and they all have the same replacement policy.
        # (And thus the same MirrorUploader.)
        for subprovider in provider.providers:
            eq_(provider.collection, subprovider.collection)
            eq_(provider.replacement_policy, subprovider.replacement_policy)

    def test_gather_providers_opds_for_distributors(self):
        collection = self._default_collection
        collection.protocol = ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        provider = IdentifierResolutionCoverageProvider(collection)

        # We got two subproviders -- one that can handle a collection
        # based on any protocol, and one that can only handle a
        # collection based on the OPDS For Distributors protocol.
        content_cafe, integration_client = provider.providers
        assert isinstance(content_cafe, ContentCafeCoverageProvider)
        assert isinstance(
            integration_client, IntegrationClientCoverImageCoverageProvider
        )
        # All subproviders are associated with the collection used in the
        # main provider, and they all have the same replacement policy.
        # (And thus the same MirrorUploader.)
        for subprovider in provider.providers:
            eq_(provider.collection, subprovider.collection)
            eq_(provider.replacement_policy, subprovider.replacement_policy)

    def test_process_item(self):
        """We handle an Identifier by making sure it's handle by all
        sub-CoverageProviders.
        """

        provider1 = object()
        provider2 = object()

        class Mock(IdentifierResolutionCoverageProvider):
            processed = []

            def providers(self):
                return [provider1, provider2]

            def process_one_provider(self, identifier, provider):
                self.processed.append((identifier, provider))

        provider = Mock(self._default_collection)
        identifier = self._identifier()
        result = provider.process_item(identifier)

        # Success!
        eq_(identifier, result)

        # A dummy LicensePool was created for this Identifier.
        [lp] = identifier.licensed_through
        eq_(DataSource.INTERNAL_PROCESSING, lp.data_source.name)
        eq_(1, lp.licenses_owned)
        eq_(1, lp.licenses_available)

        # process_one_provider was called on every known sub-provider.
        eq_([(identifier, provider1), (identifier, provider2)],
            provider.processed)

    def test_process_one_provider(self):
        """Test what happens when IdentifierResolutionCoverageProvider
        tells a subprovider to do something.
        """
        collection = self._default_collection

        provider = IdentifierResolutionCoverageProvider(
            collection, force=object()
        )

        # If the subprovider can't cover the Identifier, nothing
        # happens.
        class CantCoverAnything(object):
            def can_cover(self, identifier):
                return False
            def ensure_coverage(self, identifier, force):
                raise Exception("I'll never be called")
        provider.process_one_provider(object(), CantCoverAnything())

        # Try again with a subprovider that doesn't need coverage
        # for every collection.
        class OnlyOnce(CollectionCoverageProvider):
            SERVICE_NAME = "Do it once, it's done for every collection"
            COVERAGE_COUNTS_FOR_EVERY_COLLECTION = True
            DATA_SOURCE_NAME = DataSource.OVERDRIVE

            def register(self, identifier, collection, force):
                self.register_called_with = [identifier, collection, force]

            def ensure_coverage(self, identifier, force):
                self.ensure_coverage_called_with = [identifier, force]

        i1 = self._identifier()
        subprovider = OnlyOnce(collection)
        provider.process_one_provider(i1, subprovider)
        
        # The subprovider's register method was called, with no
        # collection being provided.
        eq_([i1, None, provider.force], subprovider.register_called_with)

        # If the main provider requires that coverage happen immediately,
        # then ensure_coverage_called_with is called instead.
        provider.provide_coverage_immediately = True
        provider.process_one_provider(i1, subprovider)
        eq_([i1, provider.force], subprovider.ensure_coverage_called_with)

        # Try again with a subprovider that _does_ need separate coverage
        # for every collection.
        class EveryTime(CollectionCoverageProvider):
            SERVICE_NAME = "Every collection must be covered separately"
            COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
            DATA_SOURCE_NAME = DataSource.OVERDRIVE

            def register(self, identifier, collection, force):
                self.register_called_with = [identifier, collection, force]

            def ensure_coverage(self, identifier, force):
                self.ensure_coverage_called_with = [identifier, force]

        subprovider = EveryTime(collection)
        provider.provide_coverage_immediately = False
        provider.process_one_provider(i1, subprovider)

        # The subprovider's register method was called, with the
        # collection we're covering being provided.
        eq_([i1, provider.collection, provider.force],
            subprovider.register_called_with)

        # If the main provider requires that coverage happen immediately,
        # then ensure_coverage_called_with is called instead.
        provider.provide_coverage_immediately = True
        provider.process_one_provider(i1, subprovider)
        eq_([i1, provider.force], subprovider.ensure_coverage_called_with)

        # In this case, collection is not provided, because
        # ensure_coverage has its own code to check
        # COVERAGE_COUNTS_FOR_EVERY_COLLECTION.

# class TestIdentifierResolutionCoverageProvider(DatabaseTest):

#     def setup(self):
#         super(TestIdentifierResolutionCoverageProvider, self).setup()
#         self.identifier = self._identifier(Identifier.OVERDRIVE_ID)
#         self._default_collection.catalog_identifier(self.identifier)
#         self.source = DataSource.license_source_for(self._db, self.identifier)

#         # Create mocks for the different collections and APIs used by
#         # IdentifierResolutionCoverageProvider.
#         self._default_collection.external_account_id = self._url
#         overdrive_collection = MockOverdriveAPI.mock_collection(self._db)
#         overdrive_collection.name = (
#             IdentifierResolutionCoverageProvider.DEFAULT_OVERDRIVE_COLLECTION_NAME
#         )
#         self.viaf = MockVIAFClient(self._db)
#         self.mirror = MockS3Uploader()
#         self.mock_content_cafe = ContentCafeAPI(
#             self._db, None, object(), object(), self.mirror
#         )

#         # Make the constructor arguments available in case a test
#         # needs to create a different type of resolver.
#         self.provider_kwargs = dict(
#             mirror=self.mirror,
#             viaf_client=self.viaf,
#             overdrive_api_class=MockOverdriveAPI,
#         )

#         # But most tests will use this resolver.
#         self.resolver = MockIdentifierResolutionCoverageProvider(
#             self._default_collection, content_cafe_api=self.mock_content_cafe,
#             **self.provider_kwargs
#         )

#         # Create some useful CoverageProviders that can be inserted
#         # into self.resolver.required_coverage_providers
#         # and self.resolver.optional_coverage_providers
#         self.always_successful = AlwaysSuccessfulCoverageProvider(self._db)
#         self.never_successful = NeverSuccessfulCoverageProvider(self._db)
#         self.broken = BrokenCoverageProvider(self._db)

#     def test_process_item_creates_license_pool(self):
#         self.resolver.required_coverage_providers = [
#             self.always_successful
#         ]

#         self.resolver.process_item(self.identifier)
#         [lp] = self.identifier.licensed_through
#         eq_(True, isinstance(lp, LicensePool))
#         eq_(lp.collection, self.resolver.collection)
#         eq_(lp.data_source, self.resolver.data_source)

#         # Prepare an identifier that already has a LicensePool through
#         # another source.
#         licensed = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
#         other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
#         lp = LicensePool.for_foreign_id(
#             self._db, other_source, licensed.type, licensed.identifier,
#             collection=self._default_collection
#         )[0]

#         self.resolver.process_item(licensed)
#         eq_([lp], licensed.licensed_through)

#     def test_process_item_uses_viaf_to_determine_author_name(self):
#         self.resolver.required_coverage_providers = [
#             self.always_successful
#         ]
#         edition = self._edition(
#             identifier_type=self.identifier.type,
#             identifier_id=self.identifier.identifier,
#             authors=['Mindy K']
#         )
#         [original_contributor] = edition.contributors
#         eq_('K, Mindy', original_contributor.sort_name)

#         self.resolver.process_item(self.identifier)
#         [lp] = self.identifier.licensed_through
#         eq_(True, isinstance(lp, LicensePool))
#         eq_(lp.collection, self.resolver.collection)
#         eq_(lp.data_source, self.resolver.data_source)

#         # Because this book had a presentation edition, a work was
#         # generated.
#         work = self.identifier.work
#         assert isinstance(work, Work)
#         eq_(edition, work.presentation_edition)
#         eq_(edition.title, work.title)

#         # VIAF updated the same contributor object with better name data.
#         eq_([original_contributor], list(edition.contributors))
#         eq_("Kaling, Mindy", original_contributor.sort_name)
#         eq_("Mindy Kaling", original_contributor.display_name)

#     def test_run_through_relevant_providers(self):
#         providers = [self.always_successful, self.never_successful]

#         # Try to process an identifier through two providers, one of
#         # which will fail.
#         success = self.resolver.run_through_relevant_providers(
#             self.identifier, providers, fail_on_any_failure=False
#         )
#         # Even though one of the providers failed, the operation as a
#         # whole succeeded.
#         eq_(None, success)

#         # Try again, under less permissive rules.
#         failure = self.resolver.run_through_relevant_providers(
#             self.identifier, providers, fail_on_any_failure=True
#         )
#         # We get a CoverageFailure representing the first failed
#         # coverage provider.
#         assert isinstance(failure, CoverageFailure)
#         eq_("500: What did you expect?", failure.exception)

#     def test_process_item_succeeds_if_all_required_coverage_providers_succeed(self):
#         # Give the identifier an edition so a work can be created.
#         edition = self._edition(
#             identifier_type=self.identifier.type,
#             identifier_id=self.identifier.identifier
#         )

#         self.resolver.required_coverage_providers = [
#             self.always_successful, self.always_successful
#         ]

#         # The coverage provider succeeded and returned an identifier.
#         result = self.resolver.process_item(self.identifier)
#         eq_(result, self.identifier)

#     def test_process_item_fails_if_any_required_coverage_providers_fail(self):
#         self.resolver.required_coverage_providers = [
#             self.always_successful, self.never_successful
#         ]
#         result = self.resolver.process_item(self.identifier)
#         eq_(True, isinstance(result, CoverageFailure))
#         eq_("500: What did you expect?", result.exception)
#         eq_(False, result.transient)

#         # The failure type of the IdentifierResolutionCoverageProvider
#         # coverage record matches the failure type of the required provider's
#         # coverage record.
#         self.never_successful.transient = True
#         result = self.resolver.process_item(self.identifier)
#         eq_(True, isinstance(result, CoverageFailure))
#         eq_(True, result.transient)

#     def test_process_item_fails_when_required_provider_raises_exception(self):
#         self.resolver.required_coverage_providers = [self.broken]
#         result = self.resolver.process_item(self.identifier)

#         eq_(True, isinstance(result, CoverageFailure))
#         eq_(True, result.transient)

#     def test_process_item_succeeds_when_optional_provider_fails(self):
#         # Give the identifier an edition so a work can be created.
#         edition = self._edition(
#             identifier_type=self.identifier.type,
#             identifier_id=self.identifier.identifier
#         )

#         self.resolver.required_coverage_providers = [
#             self.always_successful, self.always_successful
#         ]

#         self.resolver.optional_coverage_providers = [
#             self.always_successful, self.never_successful
#         ]

#         result = self.resolver.process_item(self.identifier)

#         # A successful result is achieved, even though the optional
#         # coverage provider failed.
#         eq_(result, self.identifier)

#         # An appropriate coverage record was created to mark the failure.
#         r = self._db.query(CoverageRecord).filter(
#             CoverageRecord.identifier==self.identifier,
#             CoverageRecord.operation==self.never_successful.OPERATION).one()
#         eq_("What did you expect?", r.exception)

#     def test_process_item_registers_work_for_calculation(self):
#         # Give the identifier an edition so a work can be created.
#         edition = self._edition(
#             identifier_type=self.identifier.type,
#             identifier_id=self.identifier.identifier
#         )

#         self.resolver.required_coverage_providers = [self.always_successful]
#         result = self.resolver.process_item(self.identifier)

#         eq_(result, self.identifier)

#         # The identifier has been given a work.
#         work = self.identifier.work
#         assert isinstance(work, Work)

#         # The work has a WorkCoverageRecord for presentation calculation.
#         [record] = [r for r in work.coverage_records
#                     if r.operation==WorkPresentationCoverageProvider.OPERATION]


# class TestIdentifierResolutionRegistrar(DatabaseTest):

#     PROVIDER = IdentifierResolutionCoverageProvider

#     def setup(self):
#         super(TestIdentifierResolutionRegistrar, self).setup()
#         self.registrar = IdentifierResolutionRegistrar(self._default_collection)
#         self.identifier = self._identifier()

#     def test_resolution_coverage(self):
#         # Returns None if the identifier doesn't have a coverage record
#         # for the IdentifierResolutionCoverageProvider.
#         source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
#         cr = self._coverage_record(self.identifier, source)
#         result = self.registrar.resolution_coverage(self.identifier)
#         eq_(None, result)

#         # Returns an IdentifierResolutionCoverageProvider record if it exists.
#         source = DataSource.lookup(self._db, self.PROVIDER.DATA_SOURCE_NAME)
#         cr = self._coverage_record(
#             self.identifier, source, operation=self.PROVIDER.OPERATION
#         )
#         result = self.registrar.resolution_coverage(self.identifier)
#         eq_(cr, result)

#     def test_process_item_prioritizes_collection_specific_coverage(self):
#         # The identifier has already been registered for coverage.
#         self.registrar.process_item(self.identifier)
#         original_num = len(self.identifier.coverage_records)

#         # Catalog the identifier in a collection eligible for collection-
#         # specific coverage.
#         c1 = self._collection(protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS)
#         c1.catalog_identifier(self.identifier)

#         # Reprocessing the identifier results in a new coverage record.
#         self.registrar.process_item(self.identifier)
#         expected_record_count = original_num + 1
#         eq_(expected_record_count, len(self.identifier.coverage_records))

#         # Even if resolution was successful, new collections result in
#         # registrations.
#         record = self.registrar.resolution_coverage(self.identifier)
#         record.status = CoverageRecord.SUCCESS
#         c2 = self._collection(protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS)
#         c2.catalog_identifier(self.identifier)

#         self.registrar.process_item(self.identifier)
#         expected_record_count = expected_record_count + 1
#         eq_(expected_record_count, len(self.identifier.coverage_records))

#     def test_process_item_does_not_catalog_already_cataloged_identifier(self):
#         # This identifier is already in a Collection's catalog.
#         collection = self._collection(data_source_name=DataSource.OA_CONTENT_SERVER)
#         collection.catalog.append(self.identifier)

#         # Registering it as unresolved doesn't also add it to the
#         # 'unaffiliated' Collection.
#         self.registrar.process_item(self.identifier)
#         eq_([collection], self.identifier.collections)

#     def test_process_item_catalogs_unaffiliated_identifiers(self):
#         # This identifier has no collection.
#         eq_(0, len(self.identifier.collections))

#         unaffiliated_collection, ignore = self.PROVIDER.unaffiliated_collection(self._db)
#         self.registrar.process_item(self.identifier)
#         eq_([unaffiliated_collection], self.identifier.collections)

#     def test_process_item_creates_expected_initial_coverage_records(self):

#         def assert_initial_coverage_record(record):
#             eq_(CoverageRecord.REGISTERED, record.status)
#             eq_(None, record.exception)

#         def assert_expected_coverage_records_created(
#             records, expected_source_names
#         ):
#             # The expected number of CoverageRecords was created.
#             eq_(len(expected_source_names), len(records))
#             resulting_sources = list()
#             for cr in records:
#                 # Each CoverageRecord has the expected error details.
#                 assert_initial_coverage_record(cr)
#                 resulting_sources.append(cr.data_source.name)
#             # The CoverageRecords created are for the metadata we would expect.
#             eq_(sorted(expected_source_names), sorted(resulting_sources))

#         test_cases = {
#             Identifier.OVERDRIVE_ID : [
#                 DataSource.INTERNAL_PROCESSING,
#                 DataSource.OCLC_LINKED_DATA,
#                 DataSource.OVERDRIVE,
#             ],
#             Identifier.ISBN : [
#                 DataSource.INTERNAL_PROCESSING,
#                 DataSource.CONTENT_CAFE,
#                 DataSource.OCLC_LINKED_DATA,
#             ],
#             Identifier.GUTENBERG_ID : [
#                 DataSource.INTERNAL_PROCESSING,
#                 DataSource.OCLC,
#             ]
#         }

#         for identifier_type, expected_source_names in test_cases.items():
#             # Confirm the identifier begins without coverage records.
#             eq_(0, len(self.identifier.coverage_records))

#             self.identifier.type = identifier_type
#             self.registrar.process_item(self.identifier)

#             assert_expected_coverage_records_created(
#                 self.identifier.coverage_records, expected_source_names
#             )
#             for cr in self.identifier.coverage_records:
#                 # Delete the CoverageRecords so the next tests can run.
#                 self._db.delete(cr)
#             self._db.commit()

#         # If the identifier is associated with a collection, the appropriate
#         # CoverageRecords are added.
#         opds_distrib = self._collection(
#             protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
#         )
#         opds_import = self._collection(data_source_name=DataSource.OA_CONTENT_SERVER)
#         self.identifier.collections.extend([opds_distrib, opds_import])
#         self.registrar.process_item(self.identifier)

#         # A CoverageRecord is created for the OA content server.
#         [oa_content] = [cr for cr in self.identifier.coverage_records
#                         if cr.data_source.name==DataSource.OA_CONTENT_SERVER]
#         # It doesn't have a collection, even though it used a collection
#         # to provide a DataSource.
#         eq_(None, oa_content.collection)

#         # There should be an additional DataSource.INTERNAL_PROCESSING
#         # record for the OPDS_FOR_DISTRIBUTORS coverage.
#         source_names = [cr.data_source.name
#                         for cr in self.identifier.coverage_records]
#         eq_(2, source_names.count(DataSource.INTERNAL_PROCESSING))

#     def test_process_item_creates_an_active_license_pool(self):
#         # Confirm the identifier has no LicensePool.
#         eq_([], self.identifier.licensed_through)

#         # After registration, there's a LicensePool.
#         self.registrar.process_item(self.identifier)

#         [lp] = self.identifier.licensed_through
#         eq_(lp.data_source.name, DataSource.INTERNAL_PROCESSING)
#         eq_(1, lp.licenses_owned)
#         eq_(1, lp.licenses_available)

#         # If the Identifier already has a LicensePool (highly unlikely!),
#         # no new LicensePool is created.
#         pool = self._licensepool(None)
#         identifier = pool.identifier
#         pool.licenses_owned = 4
#         pool.licenses_available = 2

#         # Registration does not create a new LicensePool.
#         self.registrar.process_item(identifier)
#         [lp] = identifier.licensed_through
#         eq_(pool, lp)

#         # It also doesn't adjust the available licenses.
#         eq_(4, lp.licenses_owned)
#         eq_(2, lp.licenses_available)
