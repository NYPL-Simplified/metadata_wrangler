from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from . import (
    DatabaseTest,
)

from core.coverage import CoverageFailure
from core.metadata_layer import ReplacementPolicy
from core.model import (
    CoverageRecord,
    ExternalIntegration,
    PresentationCalculationPolicy,
    Work,
)
from core.s3 import MockS3Uploader
from core.testing import AlwaysSuccessfulCoverageProvider

from integration_client import (
    CalculatesWorkPresentation,
    IntegrationClientCoverImageCoverageProvider,
    WorkPresentationCoverageProvider,
)


class TestWorkPresentationCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestWorkPresentationCoverageProvider, self).setup()
        self.provider = WorkPresentationCoverageProvider(self._db)

    def test_default_policy(self):
        # By default, the policy regenerates OPDS entries, in addition to
        # the usual metadata calculation items.
        original_policy = self.provider.policy
        eq_(True, original_policy.regenerate_opds_entries)
        eq_(True, original_policy.choose_edition)
        eq_(True, original_policy.set_edition_metadata)
        eq_(True, original_policy.classify)
        eq_(True, original_policy.choose_summary)
        eq_(True, original_policy.calculate_quality)
        eq_(True, original_policy.choose_cover)

    def test_policy_can_be_customized(self):
        original_policy = self.provider.policy
        new_policy = PresentationCalculationPolicy.reset_cover()

        self.provider._policy = new_policy
        eq_(new_policy, self.provider.policy)
        eq_(False, self.provider.policy.regenerate_opds_entries)
        eq_(False, self.provider.policy.choose_edition)

    def test_process_item(self):
        work = self._work()
        eq_(None, work.simple_opds_entry)
        eq_(None, work.verbose_opds_entry)
        eq_(False, work.presentation_ready)

        eq_(work, self.provider.process_item(work))

        # The OPDS entries have been calculated.
        assert work.simple_opds_entry != None
        assert work.verbose_opds_entry != None

        # The work has been made presentation-ready.
        eq_(True, work.presentation_ready)

class TestCalculatesWorkPresentation(DatabaseTest):

    # Create a mock provider that uses the mixin.
    class MockProvider(
        AlwaysSuccessfulCoverageProvider, CalculatesWorkPresentation
    ):
        pass

    def setup(self):
        super(TestCalculatesWorkPresentation, self).setup()
        self.provider = self.MockProvider(self._db)

    def test_get_work(self):
        # Without any means to a work, nothing is returned.
        identifier = self._identifier()
        eq_(None, self.provider.get_work(identifier))

        # With a means to a work (LicensePool, Edition), a work is created and
        # returned.
        edition, lp = self._edition(
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            with_license_pool=True
        )
        result = self.provider.get_work(identifier)
        assert isinstance(result, Work)
        eq_(edition.title, result.title)

        # A work that already exists can also be returned.
        eq_(result, self.provider.get_work(identifier))

    def test_no_work_found_failure(self):
        identifier = self._identifier()
        expected_msg = self.provider.INCALCULABLE_WORK % identifier

        result = self.provider.no_work_found_failure(identifier)
        assert isinstance(result, CoverageFailure)
        eq_(identifier, result.obj)
        eq_(expected_msg, result.exception)

    def test_update_work_presentation(self):
        work = self._work()
        identifier = work.presentation_edition.primary_identifier
        # The work is initialized with no coverage records.
        eq_(0, len(work.coverage_records))

        # It registers a work for presentation calculation.
        result = self.provider.update_work_presentation(work, identifier)
        eq_(None, result)
        [record] = work.coverage_records
        eq_(WorkPresentationCoverageProvider.OPERATION, record.operation)
        eq_(CoverageRecord.REGISTERED, record.status)

        # It returns the record to REGISTERED status, even if it already
        # exists.
        record.status = CoverageRecord.SUCCESS
        self.provider.update_work_presentation(work, identifier)
        eq_(CoverageRecord.REGISTERED, record.status)

        # It runs a hook method, if it's defined.
        new_title = "What's Love Gotta Do Wit It? (The 10-Part Saga)"
        class HookMethodProvider(self.MockProvider):
            def presentation_calculation_pre_hook(self, work):
                work.presentation_edition.title = new_title
        hook_provider = HookMethodProvider(self._db)

        result = hook_provider.update_work_presentation(work, identifier)
        eq_(None, result)
        eq_(new_title, work.title)

        # It returns a CoverageFailure if the hook method errors.
        class FailedHookMethodProvider(self.MockProvider):
            def presentation_calculation_pre_hook(self, work):
                raise RuntimeError("Ack!")
        failed_hook_provider = FailedHookMethodProvider(self._db)

        result = failed_hook_provider.update_work_presentation(work, identifier)
        assert isinstance(result, CoverageFailure)
        eq_(identifier, result.obj)
        assert "Ack!" in result.exception


class TestIntegrationClientCoverImageCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestIntegrationClientCoverImageCoverageProvider, self).setup()
        mirror = MockS3Uploader()
        replacement_policy = ReplacementPolicy.from_metadata_source(
            mirror=mirror
        )
        self.collection = self._collection(
            protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        )

        self.provider = IntegrationClientCoverImageCoverageProvider(
            replacement_policy=replacement_policy, collection=self.collection
        )

    def test_data_source_is_collection_specific(self):
        eq_(self.collection.name, self.provider.data_source.name)

    def test_process_item_registers_work_for_calculation(self):
        edition, lp = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        self.provider.process_item(identifier)

        work = identifier.work
        [record] = [r for r in work.coverage_records if (
                    r.operation==WorkPresentationCoverageProvider.OPERATION
                    and r.status==CoverageRecord.REGISTERED)]
