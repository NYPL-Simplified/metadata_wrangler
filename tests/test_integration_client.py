from . import (
    DatabaseTest,
)

from core.coverage import CoverageFailure
from core.model import (
    CoverageRecord,
    ExternalIntegration,
    PresentationCalculationPolicy,
    Work,
)
from core.s3 import MockS3Uploader
from core.testing import AlwaysSuccessfulCoverageProvider

from coverage_utils import MetadataWranglerReplacementPolicy
from integration_client import (
    CalculatesWorkPresentation,
    IntegrationClientCoverImageCoverageProvider,
    WorkPresentationCoverageProvider,
)


class TestWorkPresentationCoverageProvider(DatabaseTest):

    def setup_method(self):
        super(TestWorkPresentationCoverageProvider, self).setup_method()
        self.provider = WorkPresentationCoverageProvider(self._db)

    def test_default_policy(self):
        # By default, the policy regenerates OPDS entries, in addition to
        # the usual metadata calculation items.
        original_policy = self.provider.policy
        assert True == original_policy.regenerate_opds_entries
        assert True == original_policy.choose_edition
        assert True == original_policy.set_edition_metadata
        assert True == original_policy.classify
        assert True == original_policy.choose_summary
        assert True == original_policy.calculate_quality
        assert True == original_policy.choose_cover

    def test_policy_can_be_customized(self):
        original_policy = self.provider.policy
        new_policy = PresentationCalculationPolicy.reset_cover()

        self.provider._policy = new_policy
        assert new_policy == self.provider.policy
        assert False == self.provider.policy.regenerate_opds_entries
        assert False == self.provider.policy.choose_edition

    def test_process_item(self):
        work = self._work()
        assert None == work.simple_opds_entry
        assert None == work.verbose_opds_entry
        assert False == work.presentation_ready

        assert work == self.provider.process_item(work)

        # The OPDS entries have been calculated.
        assert work.simple_opds_entry != None
        assert work.verbose_opds_entry != None

        # The work has been made presentation-ready.
        assert True == work.presentation_ready

class TestCalculatesWorkPresentation(DatabaseTest):

    # Create a mock provider that uses the mixin.
    class MockProvider(
        AlwaysSuccessfulCoverageProvider, CalculatesWorkPresentation
    ):
        pass

    def setup_method(self):
        super(TestCalculatesWorkPresentation, self).setup_method()
        self.provider = self.MockProvider(self._db)

    def test_get_work(self):
        # Without any means to a work, nothing is returned.
        identifier = self._identifier()
        assert None == self.provider.get_work(identifier)

        # With a means to a work (LicensePool, Edition), a work is created and
        # returned.
        edition, lp = self._edition(
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            with_license_pool=True
        )
        result = self.provider.get_work(identifier)
        assert isinstance(result, Work)
        assert edition.title == result.title

        # A work that already exists can also be returned.
        assert result == self.provider.get_work(identifier)

    def test_no_work_found_failure(self):
        identifier = self._identifier()
        expected_msg = self.provider.INCALCULABLE_WORK % identifier

        result = self.provider.no_work_found_failure(identifier)
        assert isinstance(result, CoverageFailure)
        assert identifier == result.obj
        assert expected_msg == result.exception

    def test_update_work_presentation(self):
        work = self._work()
        identifier = work.presentation_edition.primary_identifier
        # The work is initialized with no coverage records.
        assert 0 == len(work.coverage_records)

        # It registers a work for presentation calculation.
        result = self.provider.update_work_presentation(work, identifier)
        assert None == result
        [record] = work.coverage_records
        assert WorkPresentationCoverageProvider.OPERATION == record.operation
        assert CoverageRecord.REGISTERED == record.status

        # It returns the record to REGISTERED status, even if it already
        # exists.
        record.status = CoverageRecord.SUCCESS
        self.provider.update_work_presentation(work, identifier)
        assert CoverageRecord.REGISTERED == record.status

        # It runs a hook method, if it's defined.
        new_title = "What's Love Gotta Do Wit It? (The 10-Part Saga)"
        class HookMethodProvider(self.MockProvider):
            def presentation_calculation_pre_hook(self, work):
                work.presentation_edition.title = new_title
        hook_provider = HookMethodProvider(self._db)

        result = hook_provider.update_work_presentation(work, identifier)
        assert None == result
        assert new_title == work.title

        # It returns a CoverageFailure if the hook method errors.
        class FailedHookMethodProvider(self.MockProvider):
            def presentation_calculation_pre_hook(self, work):
                raise RuntimeError("Ack!")
        failed_hook_provider = FailedHookMethodProvider(self._db)

        result = failed_hook_provider.update_work_presentation(work, identifier)
        assert isinstance(result, CoverageFailure)
        assert identifier == result.obj
        assert "Ack!" in result.exception


class TestIntegrationClientCoverImageCoverageProvider(DatabaseTest):

    def setup_method(self):
        super(TestIntegrationClientCoverImageCoverageProvider, self).setup_method()
        mirror = MockS3Uploader()
        replacement_policy = MetadataWranglerReplacementPolicy.from_db(
            self._db, mirror=mirror
        )
        self.collection = self._collection(
            protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        )

        self.provider = IntegrationClientCoverImageCoverageProvider(
            replacement_policy=replacement_policy, collection=self.collection
        )

    def test_default_replacement_policy(self):
        # In setup_method() we provide a replacement policy for use in the
        # test.  If you don't provide a replacement policy, a
        # MetadataWranglerReplacementPolicy is automatically created.
        provider = IntegrationClientCoverImageCoverageProvider(
            collection=self.collection
        )
        assert isinstance(
            provider.replacement_policy, MetadataWranglerReplacementPolicy
        )

        # Verify that links are replaced. This automatically happens
        # because of the from_metadata_source() call but we test it
        # because we used to have code that explicitly set this.
        assert True == provider.replacement_policy.links

    def test_data_source_is_collection_specific(self):
        assert self.collection.name == self.provider.data_source.name

    def test_process_item_registers_work_for_calculation(self):
        edition, lp = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        self.provider.process_item(identifier)

        work = identifier.work
        [record] = [r for r in work.coverage_records if (
                    r.operation==WorkPresentationCoverageProvider.OPERATION
                    and r.status==CoverageRecord.REGISTERED)]
