from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from core.model import (
    DataSource
)

from coverage_utils import (
    MetadataWranglerBibliographicCoverageProvider,
    ResolveVIAFOnSuccessCoverageProvider,
)

class TestMetadataWranglerBibliographicCoverageProvider(DatabaseTest):

    def test_work_created_with_internal_processing_licensepool(self):
        class Mock(MetadataWranglerBibliographicCoverageProvider):
            SERVICE_NAME = "Mock"
            DATA_SOURCE_NAME = DataSource.OVERDRIVE

        provider = Mock(self._default_collection)
        eq_(DataSource.OVERDRIVE, provider.data_source.name)

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
        eq_(DataSource.INTERNAL_PROCESSING, pool.data_source.name)


class MockResolveVIAF(ResolveVIAFOnSuccessCoverageProvider):
    SERVICE_NAME = "Mock resolve_viaf"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE

class TestResolveVIAFOnSuccessCoverageProvider(DatabaseTest):

    def test_handle_success(self):
        provider = MockResolveVIAF(self._default_collection)

    def test_handle_success_failures(self):
        """Test failures that can happen during handle_success."""

        class Mock(MockResolveVIAF):
            def resolve_viaf(self, work, client):
                raise Exception("nooo")

        provider = Mock(self._default_collection)

        # We can't create a Work for this Identifier because there's no data
        # whatsoever.
        no_data = self._identifier()
        failure = provider.handle_success(no_data)
        eq_("Work could not be calculated", failure.exception)

        # We can create a Work for this Identifier but we have a problem
        # normalizing its contributor information through VIAF.
        edition = self._edition()
        failure = provider.handle_success(edition.primary_identifier)
        assert 'nooo' in failure.exception

        # However, the Work is still presentation-ready. Even though
        # the VIAF part failed, the Work is still basically usable.
        work = edition.primary_identifier.work
        eq_(edition.title, work.title)
        eq_(True, work.presentation_ready)

    def test_resolve_viaf(self):
        class MockVIAF(object):
            processed = []
            def process_contributor(self, contributor):
                self.processed.append(contributor)

        # We did something and ended up with a functioning Work.
        work = self._work(
            authors=['Author 1', 'Author 2'], with_license_pool=True
        )
        c1, c2 = work.presentation_edition.contributors
        eq_(None, c1.sort_name)
        eq_(None, c2.sort_name)

        # Now let's call resolve_viaf().
        provider = MockResolveVIAF(self._default_collection)
        provider.viaf = MockVIAF()
        provider.resolve_viaf(work)

        # The two contributors associated with the work's presentation edition
        # were run through the MockVIAF().
        eq_([c1, c2], provider.viaf.processed)

        # Since it's just a mock, no VIAF anything actually happened.
        # But _because_ nothing happened, we made guesses as to the
        # sort names of the two contributors.
        eq_("1, Author", c1.sort_name)
        eq_("2, Author", c2.sort_name)

