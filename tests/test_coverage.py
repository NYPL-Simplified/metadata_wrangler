from nose.tools import (
    eq_,
    set_trace,
)

from . import DatabaseTest

from core.model import (
    CoverageRecord,
    DataSource,
    ExternalIntegration,
)

from core.s3 import S3Uploader
from core.coverage import CollectionCoverageProvider
from core.overdrive import MockOverdriveAPI

from content_cafe import ContentCafeCoverageProvider
from coverage import IdentifierResolutionCoverageProvider
from integration_client import IntegrationClientCoverImageCoverageProvider
from oclc.classify import IdentifierLookupCoverageProvider
from overdrive import OverdriveBibliographicCoverageProvider
from viaf import VIAFClient


class MockProvider(IdentifierResolutionCoverageProvider):
    """Mock IdentifierResolutionCoverageProvider.gather_providers
    so that it's easier to test the constructor.
    """
    def gather_providers(self, provider_kwargs):
        return ["a provider"]


class TestIdentifierResolutionCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestIdentifierResolutionCoverageProvider, self).setup()

    def test_constructor(self):
        """Test that the constructor does the right thing when
        given arguments like those used in production.

        Other tests will invoke the constructor with mock objects.
        """
        immediate = object()
        force = object()
        provider = MockProvider(
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

        # Since no storage integration is configured, we will not be mirroring
        # content anywhere.
        eq_(None, policy.mirror)

        # A VIAFClient was instantiated as well.
        assert isinstance(provider.viaf, VIAFClient)

        # The sub-providers were set up by calling gather_providers().
        eq_(["a provider"], provider.providers)

    def test_constructor_with_storage_configuration(self):
        # Set up a sitewide storage integration.
        storage = self._external_integration(
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
            username="a",
            password="b"
        )
        provider = MockProvider(self._default_collection)

        # An S3Uploader was instantiated from our storage integration
        # and stored in the ReplacementPolicy used by this
        # CoverageProvider.
        policy = provider.replacement_policy
        assert isinstance(policy.mirror, S3Uploader)
        eq_("b", policy.mirror.client._request_signer._credentials.secret_key)

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

    def test_gather_providers_no_credentials(self):
        # The OCLC provider should be there from the beginning, but the other CoverageProviders
        # require credentials, so IdentifierResolutionCoverageProvider can't configure them.
        providers = IdentifierResolutionCoverageProvider(self._default_collection).providers
        [oclc] = providers
        assert isinstance(oclc, IdentifierLookupCoverageProvider)

    def test_gather_providers_content_cafe(self):
        # Set up a Content Cafe integration
        content_cafe = self._external_integration(
            goal=ExternalIntegration.METADATA_GOAL,
            protocol=ExternalIntegration.CONTENT_CAFE,
            username="a",
            password="b"
        )

        # The OCLC provider was already configured; now there is also a ContentCafeCoverageProvider.
        provider = IdentifierResolutionCoverageProvider(self._default_collection)
        eq_(len(provider.providers), 2)
        [content_cafe] = [x for x in provider.providers if isinstance(x, ContentCafeCoverageProvider)]
        assert content_cafe

    def test_gather_providers_overdrive(self):
        # Set up an Overdrive integration.
        overdrive_collection = MockOverdriveAPI.mock_collection(self._db)
        provider_kwargs = {
            OverdriveBibliographicCoverageProvider : dict(
                api_class=MockOverdriveAPI
            )
        }
        provider = IdentifierResolutionCoverageProvider(
            overdrive_collection, provider_kwargs=provider_kwargs
        )

        # The OCLC provider was already configured; now there is also an OverdriveBibliographicCoverageProvider.
        eq_(len(provider.providers), 2)
        [overdrive] = [x for x in provider.providers if isinstance(x, OverdriveBibliographicCoverageProvider)]
        assert overdrive
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

        # The OCLC provider was already configured; now there is also an IntegrationClientCoverImageCoverageProvider.
        eq_(len(provider.providers), 2)
        [integration_client] = [x for x in provider.providers if isinstance(x, IntegrationClientCoverImageCoverageProvider)]
        assert integration_client

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

            def gather_providers(self, provider_kwargs):
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
        eq_(provider.collection, lp.collection)
        eq_(DataSource.INTERNAL_PROCESSING, lp.data_source.name)
        eq_(1, lp.licenses_owned)
        eq_(1, lp.licenses_available)

        # process_one_provider was called on every known sub-provider.
        eq_([(identifier, provider1), (identifier, provider2)],
            provider.processed)

        # If there already is a LicensePool, no new LicensePool is
        # created and we don't change its holdings.
        lp.licenses_owned = 10
        lp.licenses_available = 5
        result = provider.process_item(identifier)
        eq_([lp], identifier.licensed_through)
        eq_(10, lp.licenses_owned)
        eq_(5, lp.licenses_available)

    def test_process_item_creates_work_object_if_any_work_was_done(self):

        class JustAddMetadata(object):
            """A mock CoverageProvider that puts some data in place, but for
            whatever reason neglects to create a presentation-ready
            Work.
            """
            COVERAGE_COUNTS_FOR_EVERY_COLLECTION = True
            STATUS = CoverageRecord.SUCCESS
            SOURCE = DataSource.lookup(self._db, DataSource.GUTENBERG)
            TITLE = "A great book"
            def can_cover(self, *args, **kwargs):
                return True

            def register(s, identifier, *args, **kwargs):
                # They only told us to register, but we're going to
                # actually do the work.
                edition = self._edition(
                    identifier_type=identifier.type,
                    identifier_id=identifier.identifier,
                    title=s.TITLE
                )
                return self._coverage_record(
                    identifier, coverage_source=s.SOURCE,
                    status=s.STATUS
                ), True

        sub_provider = JustAddMetadata()
        class Mock(IdentifierResolutionCoverageProvider):
            def gather_providers(self, provider_kwargs):
                return [sub_provider]

        identifier = self._identifier()
        provider = Mock(self._default_collection)
        result = provider.process_item(identifier)

        # The IdentifierResolutionCoverageProvider completed
        # successfully.
        eq_(result, identifier)

        # Because JustAddMetadata created a CoverageRecord with the status
        # of SUCCESS, process_item decided to try and create a Work for
        # the Identifier, and was successful.
        work = identifier.work
        eq_(True, work.presentation_ready)
        eq_("A great book", work.title)
        eq_(None, work.fiction)
        eq_(None, work.audience)

        #
        # But what if that CoverageRecord hadn't been created with the
        # 'success' status? What if some work was done, but the
        # CoverageRecord was created with the 'registered' status?
        #
        sub_provider.TITLE = "Another book"
        sub_provider.STATUS = CoverageRecord.REGISTERED

        # In that case, process_item() doesn't bother trying to create
        # a Work for the Identifier, since it appears that no work was
        # actually done.
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(result, identifier)
        eq_(None, identifier.work)

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
                return None, None

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
                return None, None

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
