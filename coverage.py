from nose.tools import set_trace

from core.coverage import (
    CoverageFailure, 
    CoverageProvider, 
)

from core.metadata_layer import (
    ReplacementPolicy, 
)

from core.model import (
    CoverageRecord, 
    DataSource, 
    get_one_or_create,
    Identifier, 
    PresentationCalculationPolicy, 
)

from core.overdrive import (
    OverdriveBibliographicCoverageProvider, 
)

from core.s3 import (
    S3Uploader, 
)

from overdrive import (
    OverdriveCoverImageMirror, 
)

from content_cafe import (
    ContentCafeCoverageProvider, 
)

from content_server import (
    ContentServerCoverageProvider, 
)

from gutenberg import (
    OCLCClassifyCoverageProvider, 
)

from mirror import ImageScaler

from oclc import LinkedDataCoverageProvider

from viaf import (
    VIAFClient, 
)


'''
class ResolutionFailed(Exception):
    """Indicates a failure in identifier resolution."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return "%s: %s" % (self.status_code, self.message)
'''


class IdentifierResolutionCoverageProvider(CoverageProvider):
    """ Resolve all of the Identifiers with CoverageProviders in transient 
    failure states, turning them into Editions with LicensePools.
    Create CoverageProviders to contact 3rd party entities for information on 
    Identifier-represented library item (book).

    For ISBNs, make a bunch of Resources, rather than LicensePooled Editions.
    """

    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, _db, batch_size=10, cutoff_time=None, operation=None):
        # Since we are the metadata wrangler, any resources we find,
        # we mirror to S3.
        mirror = S3Uploader()

        output_source, made_new = get_one_or_create(
            _db, DataSource,
            name=DataSource.INTERNAL_PROCESSING,
            offers_licenses=False,
        )

        input_identifier_types = [Identifier.OVERDRIVE_ID, Identifier.ISBN]

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        presentation_calculation_policy = PresentationCalculationPolicy(
            regenerate_opds_entries=True,
            update_search_index=True
        )
        policy = ReplacementPolicy.from_metadata_source(
            mirror=mirror, even_if_not_apparently_updated=True,
            presentation_calculation_policy=presentation_calculation_policy
        )
        overdrive = OverdriveBibliographicCoverageProvider(
            _db, metadata_replacement_policy=policy
        )
        content_cafe = ContentCafeCoverageProvider(_db)
        content_server = ContentServerCoverageProvider(_db)
        oclc_classify = OCLCClassifyCoverageProvider(_db)

        self.optional_coverage_providers = []
        self.required_coverage_providers = [overdrive, content_cafe, content_server, oclc_classify]

        super(IdentifierResolutionCoverageProvider, self).__init__(
            service_name="Identifier Resolution Coverage Provider", 
            input_identifier_types=input_identifier_types,
            output_source = output_source,
            batch_size=batch_size,
            operation=CoverageRecord.RESOLVE_IDENTIFIER_OPERATION, 
        )

        self.viaf = VIAFClient(self._db)
        self.image_mirrors = {
            DataSource.OVERDRIVE : OverdriveCoverImageMirror(self._db)
        }
        self.image_scaler = ImageScaler(self._db, self.image_mirrors.values())
        self.oclc_linked_data = LinkedDataCoverageProvider(self._db)


    def process_item(self, identifier):
        """ For this identifier, checks that it has all of the 
        available 3rd party metadata, and if not, obtains it.
        If metadata failed to be obtained, and the coverage was deemed required, 
        then returns a CoverageFailure.
        """
        self.log.info("Ensuring coverage for %r", identifier)

        # Go through all relevant providers and tries to ensure coverage.
        # If there's a failure or an exception, raise ResolutionFailed.
        for provider in self.required_coverage_providers:
            if not identifier.type in provider.input_identifier_types:
                continue
            record = provider.ensure_coverage(identifier, force=True)
            if record.exception:
                error_msg = "500; " + record.exception
                transiency = True
                if record.status == CoverageRecord.PERSISTENT_FAILURE:
                    transiency = False
                return CoverageFailure(identifier, error_msg, data_source=self.output_source, transient=transiency)

        # Now go through the optional providers. It's the same deal,
        # but a CoverageFailure doesn't cause the entire identifier
        # resolution process to fail.
        for provider in self.optional_coverage_providers:
            if not identifier.type in provider.input_identifier_types:
                continue
            record = provider.ensure_coverage(identifier, force=True)

        # TODO: what if process_work raises an exception?
        #  If finalize() raises an
        # exception the process could still fail and need to be
        # retried.
        self.resolve_equivalent_oclc_identifiers(identifier)
        if identifier.type==Identifier.ISBN:
            # Currently we don't try to create Works for ISBNs,
            # we just make sure all the Resources associated with the
            # ISBN are properly handled. At this point, that has
            # completed successfully, so do nothing.
            pass
        else:
            self.process_work(identifier)

        return identifier


    def process_work(self, identifier):
        """Fill in VIAF data and cover images where possible before setting
        a previously-unresolved identifier's work as presentation ready."""

        work = None
        license_pool = identifier.licensed_through
        if license_pool:
            work, created = license_pool.calculate_work(even_if_no_author=True)
        if work:
            self.resolve_viaf(work)
            self.resolve_cover_image(work)
            work.calculate_presentation()
            work.set_presentation_ready()
        else:
            '''
            raise ResolutionFailed(
                500,
                "Work could not be calculated for %r" % unresolved_identifier.identifier,
            )
            '''
            error_msg = "500; " + "Work could not be calculated for %r" % identifier
            transiency = True
            return CoverageFailure(identifier, error_msg, data_source=self.output_source, transient=transiency)


    def resolve_equivalent_oclc_identifiers(self, identifier):
        """Ensures OCLC coverage for an identifier.

        This has to be called after the OCLCClassify coverage is run to confirm
        that equivalent OCLC identifiers are available.
        """
        oclc_ids = set()
        types = [Identifier.OCLC_WORK, Identifier.OCLC_NUMBER, Identifier.ISBN]
        for edition in identifier.primarily_identifies:
            oclc_ids = oclc_ids.union(
                edition.equivalent_identifiers(type=types)
            )
        for oclc_id in oclc_ids:
            self.log.info("Currently processing equivalent identifier: %r", oclc_id)
            self.oclc_linked_data.ensure_coverage(oclc_id)

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors."""
        viaf = VIAFClient(self._db)
        for pool in work.license_pools:
            edition = pool.presentation_edition
            for contributor in edition.contributors:
                viaf.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())

    def resolve_cover_image(self, work):
        """Make sure we have the cover for all editions."""
        for pool in work.license_pools:
            edition = pool.presentation_edition
            data_source_name = edition.data_source.name
            if data_source_name in self.image_mirrors:
                self.image_mirrors[data_source_name].mirror_edition(edition)
                self.image_scaler.scale_edition(edition)





