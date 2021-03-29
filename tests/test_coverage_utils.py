from . import (
    DatabaseTest,
)

from core.coverage import CoverageFailure

from core.model import (
    DataSource,
    ExternalIntegration,
    ExternalIntegrationLink,
    Work,
)
from core.mirror import MirrorUploader
from core.tests.test_s3 import S3UploaderTest

from coverage_utils import (
    MetadataWranglerBibliographicCoverageProvider,
    MetadataWranglerReplacementPolicy,
    ResolveVIAFOnSuccessCoverageProvider,
)

class MockProvider(MetadataWranglerBibliographicCoverageProvider):
    """A simple MetadataWranglerBibliographicCoverageProvider
    for use in tests.
    """
    SERVICE_NAME = "Mock"
    DATA_SOURCE_NAME = DataSource.GUTENBERG

class TestMetadataWranglerReplacementPolicy(DatabaseTest):

    def test_from_db(self):
        """The default replacement policy for all metadata wrangler
        bibliographic coverage providers treats the data source
        as a source of metadata, and knows about any
        configured site-wide MirrorUploader.
        """

        # First, try with no storage integration configured.
        m = MetadataWranglerReplacementPolicy.from_db
        policy = m(self._db)

        # The sort of thing you expect from a metadata source.
        assert True == policy.subjects
        assert True == policy.contributions

        # But no configured MirrorUploaders.
        assert all([x==None for x in list(policy.mirrors.values())])

        # Now configure a storage integration and try again.
        integration = self._external_integration(
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
            username="username", password="password"
        )

        # A MirrorUploader for that integration was created and
        # associated with the policy.
        policy = m(self._db)
        mirrors = policy.mirrors
        uploader = mirrors[ExternalIntegrationLink.COVERS]
        assert isinstance(uploader, MirrorUploader)

        # The MirrorUploader has been properly configured with the
        # secret key from the integration.
        assert "password" == uploader.client._request_signer._credentials.secret_key

        # But the MirrorUploader is only to be used for book covers. The
        # metadata wrangler ignores the books themselves.
        assert None == mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS]


class TestMetadataWranglerBibliographicCoverageProvider(S3UploaderTest):

    def test__default_replacement_policy(self):
        # The default replacement policy is a
        # MetadataWranglerReplacementPolicy.
        mock_mirror = object()
        provider = MockProvider(self._default_collection)
        policy = provider._default_replacement_policy(
            self._db, mirror=mock_mirror
        )
        assert isinstance(
            policy, MetadataWranglerReplacementPolicy
        )
        assert mock_mirror == policy.mirrors[ExternalIntegrationLink.COVERS]

    def test_work_created_with_internal_processing_licensepool(self):
        class Mock(MetadataWranglerBibliographicCoverageProvider):
            SERVICE_NAME = "Mock"
            DATA_SOURCE_NAME = DataSource.OVERDRIVE

        provider = Mock(self._default_collection)
        assert DataSource.OVERDRIVE == provider.data_source.name

        # Ordinarily, if an Overdrive CoverageProvider needs to create
        # a work for an Identifier, and there's no LicensePool, it
        # will create an Overdrive LicensePool.
        #
        # But on the metadata wrangler, LicensePools are stand-ins
        # that don't represent actual copies of the book, so this
        # CoverageProvider creates an INTERNAL_PROCESSING LicensePool
        # instead.
        edition = self._edition()
        work = provider.work(edition.primary_identifier)
        [pool] = work.license_pools
        assert DataSource.INTERNAL_PROCESSING == pool.data_source.name

        # The dummy pool is created as an open-access LicensePool so
        # that multiple LicensePools can share the same work.
        assert True == pool.open_access

    def test_handle_success_fails_if_work_cant_be_created(self):

        class CantCreateWork(MetadataWranglerBibliographicCoverageProvider):
            SERVICE_NAME = "Mock"
            DATA_SOURCE_NAME = DataSource.GUTENBERG

            def work(self, identifier):
                return self.failure(identifier, "Can't create work.")

        provider = CantCreateWork(self._default_collection)
        pool = self._licensepool(None)

        # We successfully processed the Identifier...
        failure = provider.handle_success(pool.identifier)

        # ...but were unable to create the Work. The result is failure.
        assert isinstance(failure, CoverageFailure)
        assert "Can't create work." == failure.exception

    def test_handle_success_sets_new_work_presentation_ready(self):
        provider = MockProvider(self._default_collection)
        pool = self._licensepool(None)
        pool.open_access = False
        provider.handle_success(pool.identifier)

        # The LicensePool was forced to be open-access.
        assert True == pool.open_access

        # A presentation-ready work was created for it.
        work = pool.work
        assert True == work.presentation_ready

    def test_handle_success_recalculates_presentation_of_existing_work(self):
        """If work() returns a Work that's already presentation-ready,
        calculate_presentation() is called on the Work.
        """

        class MockWork(Work):
            """Act like a presentation-ready work that just needs
            calculate_presentation() to be called.
            """
            def __init__(self):
                self.presentation_ready = True
                self.calculate_presentation_called = False
                self.set_presentation_ready_called = False

            def calculate_presentation(self):
                self.calculate_presentation_called = True

            def set_presentation_ready(self, *args, **kwargs):
                self.set_presentation_ready_called = True

        work = MockWork()

        class Mock(MockProvider):
            def work(self, identifier):
                return work

        provider = Mock(self._default_collection)
        pool = self._licensepool(None)
        provider.handle_success(pool.identifier)

        # Since work.presentation_ready was already True,
        # work.calculate_presentation() was called.
        assert True == work.calculate_presentation_called

        # work.set_presentation_ready() was called, just to
        # be safe.
        assert True == work.set_presentation_ready_called

class MockResolveVIAF(ResolveVIAFOnSuccessCoverageProvider):
    SERVICE_NAME = "Mock resolve_viaf"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE

class TestResolveVIAFOnSuccessCoverageProvider(DatabaseTest):

    def test_handle_success(self):
        provider = MockResolveVIAF(self._default_collection)

    def test_handle_success_failures(self):
        # Test failures that can happen during handle_success.

        class Mock(MockResolveVIAF):
            def resolve_viaf(self, work):
                raise Exception("nooo")

        provider = Mock(self._default_collection)

        # We can create a Work for this Identifier, even though we
        # have no information about it, but we have a problem
        # normalizing its contributor information through VIAF.
        edition = self._edition()
        failure = provider.handle_success(edition.primary_identifier)
        assert 'nooo' in failure.exception

        # However, the Work is still presentation-ready. Even though
        # the VIAF part failed, the Work is still basically usable.
        work = edition.primary_identifier.work
        assert edition.title == work.title
        assert True == work.presentation_ready

    def test_resolve_viaf(self):
        class MockVIAF(object):
            processed = []
            def process_contributor(self, contributor):
                self.processed.append(contributor)

        # We did something and ended up with a functioning Work.
        work = self._work(
            authors=['Author 1', 'Author 2'], with_license_pool=True
        )
        c1, c2 = sorted(
            work.presentation_edition.contributors,
            key=lambda x: x.sort_name
        )
        assert "1, Author" == c1.sort_name
        assert "2, Author" == c2.sort_name

        # However, (let's say) we were not able to find the display
        # names of the contributors, only the sort names.
        c1.display_name = None
        c2.display_name = None

        # Now let's call resolve_viaf().
        provider = MockResolveVIAF(self._default_collection)
        provider.viaf = MockVIAF()
        provider.resolve_viaf(work)

        # The two contributors associated with the work's presentation edition
        # were run through the MockVIAF().
        assert set([c1, c2]) == set(provider.viaf.processed)

        # Since it's just a mock, no VIAF anything actually happened.
        # But _because_ nothing happened, we made guesses as to the
        # display names of the two contributors.
        assert "Author 1" == c1.display_name
        assert "Author 2" == c2.display_name
