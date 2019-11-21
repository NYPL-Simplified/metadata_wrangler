import logging
from nose.tools import set_trace

from sqlalchemy.orm.session import Session

from core.config import CannotLoadConfiguration
from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure
)
from core.metadata_layer import ReplacementPolicy
from core.mirror import MirrorUploader
from core.model import (
    get_one,
    DataSource,
    ExternalIntegration,
    ExternalIntegrationLink,
    Work,
)

class MetadataWranglerReplacementPolicy(ReplacementPolicy):
    """A ReplacementPolicy that uses the only configured storage
    integration as its cover mirror.
    """
    @classmethod
    def from_db(cls, _db, **kwargs):
        """Create a MetadataWranglerReplacementPolicy based on
        database settings -- specifically, with the configured
        MirrorUploader for cover images.

        :param mirror: Pass in a mock MirrorUploader to use it
        instead of creating a real one.
        """
        mirror = kwargs.pop('mirror', None)
        if not mirror:
            integration = get_one(
                _db, ExternalIntegration, goal=ExternalIntegration.STORAGE_GOAL
            )
            if integration:
                mirror = MirrorUploader.implementation(integration)
            else:
                logging.error(
                    "No storage integration is configured. Cover images will not be mirrored. You really ought to set up a storage integration."
                )
                mirror = None
        mirrors = { ExternalIntegrationLink.COVERS : mirror,
                    ExternalIntegrationLink.BOOKS : None }
        return cls.from_metadata_source(
            mirrors=mirrors, **kwargs
        )


class MetadataWranglerBibliographicCoverageProvider(BibliographicCoverageProvider):

    def _default_replacement_policy(self, _db, **kwargs):
        """In general, data used by the metadata wrangler is a reliable source
        of metadata but not of licensing information. We always
        provide the MirrorUploader in case a data source has cover
        images available.
        """
        return MetadataWranglerReplacementPolicy.from_db(_db, **kwargs)

    def work(self, identifier):
        """Create or find a Work for the given Identifier.

        If necessary, create a LicensePool for it as well.
        """
        # There should already be a dummy LicensePool, created by
        # IdentifierResolutionCoverageProvider, which we can use as a
        # basis for a work.
        licensepools = identifier.licensed_through
        if licensepools:
            license_pool = licensepools[0]
        else:
            # Even if not, we can create our own LicensePool -- it's just
            # a stand-in and doesn't represent any actual licenses.
            #
            # This may happen because a migration script created work
            # for this Identifier without going through
            # IdentifierResolutionCoverageProvider.
            license_pool = self.license_pool(
                identifier, data_source=DataSource.INTERNAL_PROCESSING
            )
            if not license_pool.licenses_owned:
                license_pool.update_availability(1, 1, 0, 0)

        # Making the dummy LicensePool open-access will ensure that
        # when multiple collections have the same book, they'll
        # all share a Work.
        license_pool.open_access = True

        # If the Identifier is already associated with a Work (because
        # we went through this process for another LicensePool for the
        # same identifier), we can reuse that Work and avoid a super()
        # call, which will wastefully destroy the old Work and create
        # an identical new one.
        #
        # Normally this isn't necessary because
        # COVERAGE_COUNTS_FOR_EVERY_COLLECTION. But migration scripts
        # may register seemingly redundant work to be done, and if
        # that happens, we don't need to create a whole other Work
        # -- we just need to recalculcate its presentation, which will
        # happen in handle_success().
        existing_work = identifier.work
        if existing_work:
            license_pool.work = existing_work
            return existing_work

        return super(
            MetadataWranglerBibliographicCoverageProvider, self).work(
                identifier, license_pool, even_if_no_title=True
            )

    def handle_success(self, identifier):
        """Try to create a new presentation-ready Work based on metadata
        obtained during process_item().

        If a Work already existed, recalculate its presentation to
        incorporate the new metadata.
        """
        work = self.work(identifier)
        if not isinstance(work, Work):
            return work

        if work.presentation_ready:
            # This work was already presentation-ready, which means
            # its presentation probably just changed and needs to be
            # recalculated.
            work.calculate_presentation()
        self.set_presentation_ready(identifier)


class ResolveVIAFOnSuccessCoverageProvider(MetadataWranglerBibliographicCoverageProvider):
    """A mix-in class for metadata wrangler BibliographicCoverageProviders
    that add author information. When such a coverage provider
    completes its work, it should run any Contributors associated with
    the presentation Edition through VIAF. Then it should try to
    create a presentation-ready work.

    By the time handle_success is called, instances of this class must
    have self.viaf set to a VIAFClient.
    """
    def handle_success(self, identifier):
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work
        work.set_presentation_ready()
        try:
            self.resolve_viaf(work)
        except Exception, e:
            message = "Exception updating VIAF coverage: %r" % e
            return self.failure(identifier, message, transient=True)
        return identifier

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors to the Work's presentation edition.
        """
        for pool in work.license_pools:
            edition = pool.presentation_edition
            if not edition:
                continue
            for contributor in edition.contributors:
                # TODO: We need some way of not going to VIAF over and over
                # again for the same contributors.
                self.viaf.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names()
                    )
