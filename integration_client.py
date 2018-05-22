from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from core.model import (
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    PresentationCalculationPolicy,
)
from core.coverage import (
    CatalogCoverageProvider,
    WorkCoverageProvider,
)
from core.metadata_layer import (
    Metadata,
    ReplacementPolicy,
)
from core.mirror import MirrorUploader


class WorkPresentationCoverageProvider(WorkCoverageProvider):

    """A CoverageProvider to reset the presentation for a Work as it
    achieves metadata coverage.
    """

    SERVICE_NAME = "Work Presentation Coverage Provider"
    OPERATION = "recalculate-presentation"
    DEFAULT_BATCH_SIZE = 25

    _policy = None

    def __init__(self, *args, **kwargs):
        if not 'registered_only' in kwargs:
            kwargs['registered_only'] = True
        super(WorkPresentationCoverageProvider, self).__init__(*args, **kwargs)

    @property
    def policy(self):
        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not calculated
        # at all, or new metadata has been added that may impact the work, or
        # something went wrong trying to calculate it last time.
        if not self._policy:
            self._policy = PresentationCalculationPolicy(
                regenerate_opds_entries=True
            )
        return self._policy

    def process_item(self, work):
        work.calculate_presentation(
            policy=self.policy, exclude_search=True,
            default_fiction=None, default_audience=None,
        )
        work.set_presentation_ready(exclude_search=True)


class CalculatesWorkPresentation(object):

    """A mixin for IdentifierCoverageProvider (and its subclasses) that
    registers a Work to have its presentation calculated or recalculated.
    """

    INCALCULABLE_WORK = "500: Work could not be calculated for %r"

    def register_work_for_calculation(self, identifier):
        """Registers the given identifier's work for presentation calculation
        with the WorkPresentationCoverageProvider.

        :return: None, if successful, or CoverageFailure
        """
        work = self.get_work(identifier)
        if not work:
            return self.no_work_found_failure(identifier)

        failure = self.update_work_presentation(work, identifier)
        if failure:
            return failure

    def get_work(self, identifier):
        """Finds or calculates a work for a given identifier.

        :return: Work or None
        """
        work = identifier.work
        if work:
            return work

        # Calculate the work directly from a LicensePool.
        license_pools = identifier.licensed_through
        if license_pools:
            pool = license_pools[0]
            work, created = pool.calculate_work(
                even_if_no_author=True, exclude_search=True,
            )
            if work:
                return work

        # A work couldn't be found or created.
        return None

    def no_work_found_failure(self, identifier):
        """Returns a CoverageFailure in the case that a Work can't be
        found or created for an identifier.
        """
        return self.failure(identifier, self.INCALCULABLE_WORK % identifier)

    def update_work_presentation(self, work, identifier):
        """Register this work to have its presentation calculated

        :return: None, if successful, or CoverageFailure
        """
        try:
            self.presentation_calculation_pre_hook(work)
        except Exception as e:
            return self.failure(identifier, repr(e), transient=True)
        WorkPresentationCoverageProvider.register(work, force=True)

    def presentation_calculation_pre_hook(self, work):
        """An optional hook method to prepare the discovered Work for
        presentation calculation.
        """
        pass


class IntegrationClientCoverImageCoverageProvider(CatalogCoverageProvider,
    CalculatesWorkPresentation
):
    """Mirrors and scales cover images we heard about from an IntegrationClient."""

    SERVICE_NAME = "Integration Client Coverage Provider"
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING

    OPERATION = CoverageRecord.IMPORT_OPERATION
    PROTOCOL = ExternalIntegration.OPDS_FOR_DISTRIBUTORS
    COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

    def __init__(self, collection, *args, **kwargs):
        _db = Session.object_session(collection)
        uploader = kwargs.pop('uploader', None) or MirrorUploader.sitewide(_db)
        self.replacement_policy = ReplacementPolicy(
            mirror=uploader, links=True
        )

        # Only process identifiers that have been registered for coverage.
        kwargs['registered_only'] = kwargs.get('registered_only', True)
        super(IntegrationClientCoverImageCoverageProvider, self).__init__(
            collection, *args, **kwargs
        )

    @property
    def data_source(self):
        """Use the collection's name as the data source name."""
        return DataSource.lookup(self._db, self.collection.name, autocreate=True)

    def process_item(self, identifier):
        edition = self.edition(identifier)
        metadata = Metadata.from_edition(edition)
        metadata.apply(edition, self.collection,
                       replace=self.replacement_policy)

        failure = self.register_work_for_calculation(identifier)
        if failure:
            return failure

        return identifier
