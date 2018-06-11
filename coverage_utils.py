from nose.tools import set_trace
from core.coverage import (
    BibliographicCoverageProvider,
    CoverageFailure
)
from core.model import DataSource

class MetadataWranglerBibliographicCoverageProvider(BibliographicCoverageProvider):

    def work(self, identifier):
        # There should already be a dummy LicensePool, created by
        # IdentifierResolutionCoverageProvider, which we can use as a
        # basis for a work.
        licensepools = identifier.licensed_through
        if licensepools:
            license_pool = licensepools[0]
        else:
            # Even if not, we can create our own LicensePool -- it's just
            # a stand-in and doesn't represent any actual licenses.
            license_pool = self.license_pool(
                identifier, data_source=DataSource.INTERNAL_PROCESSING
            )
            if not license_pool.licenses_owned:
                license_pool.update_availability(1, 1, 0, 0)

        return super(
            MetadataWranglerBibliographicCoverageProvider, self).work(
                identifier, license_pool, even_if_no_title=True
            )


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
