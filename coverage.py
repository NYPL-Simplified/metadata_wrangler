from nose.tools import set_trace

from core.coverage import (
    CoverageFailure, 
    CatalogCoverageProvider, 
)

from core.metadata_layer import (
    ReplacementPolicy, 
)

from core.model import (
    Collection,
    CoverageRecord, 
    DataSource, 
    get_one_or_create,
    Identifier, 
    PresentationCalculationPolicy, 
)

from core.overdrive import (
    OverdriveBibliographicCoverageProvider,
    OverdriveAPI,
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
    LookupClientCoverageProvider, 
)

from oclc_classify import (
    OCLCClassifyCoverageProvider, 
)

from mirror import ImageScaler

from oclc import (
    LinkedDataCoverageProvider,
)

from viaf import (
    VIAFClient, 
)


class IdentifierResolutionCoverageProvider(CatalogCoverageProvider):
    """Make sure all Identifiers registered as needing coverage by this
    CoverageProvider become Works with Editions and (probably dummy)
    LicensePools.

    Coverage happens by running the Identifier through _other_
    CoverageProviders, filling in the blanks with additional data from
    third-party entities.

    For ISBNs, we end up with a bunch of Resources, rather than
    Works. TODO: This needs to change.
    """

    SERVICE_NAME = "Identifier Resolution Coverage Provider"
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING
    INPUT_IDENTIFIER_TYPES = [Identifier.OVERDRIVE_ID, Identifier.ISBN]
    OPERATION = CoverageRecord.RESOLVE_IDENTIFIER_OPERATION
    
    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    def __init__(self, collection, uploader=None,
                 viaf_client=None, linked_data_coverage_provider=None,
                 content_cafe_api=None,
                 overdrive_api_class=OverdriveAPI,
                 **kwargs):

        super(IdentifierResolutionCoverageProvider, self).__init__(
            collection, **kwargs
        )

        # Since we are the metadata wrangler, any resources we find,
        # we mirror to S3.
        if not uploader:
            uploader = S3Uploader()

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        presentation_calculation_policy = PresentationCalculationPolicy(
            regenerate_opds_entries=True,
            update_search_index=True
        )
        policy = ReplacementPolicy.from_metadata_source(
            mirror=uploader, even_if_not_apparently_updated=True,
            presentation_calculation_policy=presentation_calculation_policy
        )

        self.overdrive_api_class = overdrive_api_class

        self.uploader = uploader
        self.content_cafe_api = content_cafe_api
        
        # Determine the optional and required coverage providers.
        # Each Identifier in this Collection's catalog will be run
        # through all relevant providers.
        self.required_coverage_providers, self.optional_coverage_providers = self.providers()

        # When we need to look up a contributor via VIAF we will use this
        # client.
        self.viaf_client = viaf_client or VIAFClient(self._db)
                
        # Books are not looked up in OCLC Linked Data directly, since
        # there is no Collection that identifies a book by its OCLC Number.
        # However, when a book is looked up through OCLC Classify, some
        # OCLC Numbers may be associated with it, and _those_ numbers
        # can be run through OCLC Linked Data.
        #
        # TODO: We get many books identified by ISBN, and those books
        # _could_ be run through a LinkedDataCoverageProvider if it
        # worked a little differently. However, I don't think this
        # would be very useful, since those books will get looked up
        # through OCLC Classify, which will probably result in us
        # finding that same ISBN via OCLC Number.
        self.oclc_linked_data = (
            linked_data_coverage_provider or
            LinkedDataCoverageProvider(self._db, viaf_api=self.viaf_client)
        )
        
        # The ordinary OverdriveBibiliographicCoverageProvider
        # doesn't upload images, so we need to create our own
        # mirror and scaler.
        #
        # TODO: This class would be neater if we were to subclass
        # OverdriveBibliographicCoverageProvider to do the scaling and
        # uploading.
        self.image_mirrors = {
            DataSource.OVERDRIVE : OverdriveCoverImageMirror(
                self._db, uploader=uploader
            )
        }
        self.image_scaler = ImageScaler(
            self._db, self.image_mirrors.values(), uploader=uploader
        )
        
    def providers(self):
        """Instantiate required and optional CoverageProviders.

        All Identifiers in this Collection's catalog will be run
        through each provider. If an optional provider fails, nothing
        will happen.  If a required provider fails, the coverage
        operation as a whole will fail.

        NOTE: This method creates CoverageProviders that go against
        real servers. Because of this, tests must use a subclass that
        mocks providers(), such as
        MockIdentifierResolutionCoverageProvider.
        """
        # All books must be run through Content Cafe and OCLC
        # Classify, assuming their identifiers are of the right
        # type.
        content_cafe = ContentCafeCoverageProvider(
            self._db, api=self.content_cafe_api, uploader=self.uploader
        )
        oclc_classify = OCLCClassifyCoverageProvider(self._db)

        optional = []
        required = [content_cafe, oclc_classify]
            
        # All books derived from OPDS import against the open-access
        # content server must be looked up in that server.
        #
        # TODO: This could stand some generalization. Any OPDS server
        # that also supports the lookup protocol can be used here.
        if (self.collection.protocol == Collection.OPDS_IMPORT
            and self.collection.data_source
            and self.collection.data_source.name == DataSource.OA_CONTENT_SERVER):
            required.append(LookupClientCoverageProvider(self.collection))

        # All books obtained from Overdrive must be looked up via the
        # Overdrive API.
        if self.collection.protocol == Collection.OVERDRIVE:
            required.append(
                OverdriveBibliographicCoverageProvider(
                    self.collection, api_class=self.overdrive_api_class
                )
            )
        return optional, required

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all identifiers lacking coverage from this CoverageProvider.

        Only identifiers that have CoverageRecords in the 'transient
        failure' state will be returned. Unlike with other
        CoverageProviders, Identifiers that have no CoverageRecord at
        all will not be processed.
        """
        qu = super(IdentifierResolutionCoverageProvider, self).items_that_need_coverage(
            identifiers=identifiers, **kwargs
        )
        qu = qu.filter(CoverageRecord.id != None)
        return qu
            
    def process_item(self, identifier):
        """For this identifier, checks that it has all of the available
        3rd party metadata, and if not, obtains it.

        If metadata failed to be obtained, and the coverage was deemed
        required, then returns a CoverageFailure.
        """
        self.log.info("Ensuring coverage for %r", identifier)

        # Make sure there's a LicensePool for this Identifier in this
        # Collection. Since we're the metadata wrangler, the
        # LicensePool will probably be a stub that doesn't actually
        # represent the right to loan the book, but that's okay.
        license_pool = self.license_pool(identifier)

        # Go through all relevant providers and try to ensure coverage.
        failure = self.run_through_relevant_providers(
            identifier, self.required_coverage_providers,
            fail_on_any_failure=True
        )
        if failure:
            return failure

        # Now go through relevant optional providers and try to ensure
        # coverage.
        failure = self.run_through_relevant_providers(
            identifier, self.optional_coverage_providers,
            fail_on_any_failure=False
        )
        if failure:
            return failure

        # We got coverage from all the required coverage providers,
        # and none of the optional coverage providers raised an exception,
        # so we're ready.
        try:
            self.finalize(identifier)
        except Exception as e:
            return self.transform_exception_into_failure(e, identifier)

        return identifier

    def run_through_relevant_providers(self, identifier, providers,
                                       fail_on_any_failure):
        """Run the given Identifier through a set of CoverageProviders.

        :param identifier: Process this Identifier.
        :param providers: Run `identifier` through every relevant
            CoverageProvider in this list.
        :param fail_on_any_failure: True means that each
            CoverageProvider must succeed or the whole operation
            fails. False means that if a CoverageProvider fails it's
            not a deal-breaker.
        :return: A CoverageFailure if there was an unrecoverable failure,
            None if everything went okay.
        """
        for provider in providers:
            if (provider.input_identifier_types
                and not identifier.type in provider.input_identifier_types):
                # The CoverageProvider under consideration doesn't
                # handle Identifiers of this type.
                continue
            try:
                record = provider.ensure_coverage(identifier, force=True)
                if fail_on_any_failure and record.exception:
                    # As the CoverageProvider under consideration has
                    # fallen, so must this CoverageProvider also fall.
                    error_msg = "500: " + record.exception
                    transient = (
                        record.status == CoverageRecord.TRANSIENT_FAILURE
                    )
                    return self.failure(
                        identifier, error_msg, transient=transient
                    )                
            except Exception as e:
                # An uncaught exception becomes a CoverageFailure no
                # matter what.
                return self.transform_exception_into_failure(e, identifier)

        # Return None to indicate success.
        return None

    def transform_exception_into_failure(self, error, identifier):
        """Ensures coverage of a given identifier by a given provider with
        appropriate error handling for broken providers.
        """
        self.log.warn(
            "Error completing coverage for %r: %r", identifier, error,
            exc_info=error
        )
        return self.failure(identifier, repr(error), transient=True)

    def finalize(self, identifier):
        """Sets equivalent identifiers from OCLC and processes the work."""

        self.resolve_equivalent_oclc_identifiers(identifier)
        if identifier.type==Identifier.ISBN:
            # Currently we don't try to create Works for ISBNs,
            # we just make sure all the Resources associated with the
            # ISBN are properly handled. At this point, that has
            # completed successfully, so do nothing.
            pass
        else:
            self.process_work(identifier)

    def process_work(self, identifier):
        """Fill in VIAF data and cover images where possible before setting
        a previously-unresolved identifier's work as presentation ready.

        TODO: I think this should be split into a separate
        WorkCoverageProvider which runs last. That way we have a record
        of which Works have had this service.
        """
        work = None
        license_pools = identifier.licensed_through
        if license_pools:
            pool = license_pools[0]
            work, created = pool.calculate_work(even_if_no_author=True)
        if work:
            self.resolve_viaf(work)
            self.resolve_cover_image(work)
            work.calculate_presentation()
            work.set_presentation_ready()
        else:
            error_msg = "500; " + "Work could not be calculated for %r" % identifier
            return self.failure(identifier, error_msg, transient=True)

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

        for pool in work.license_pools:
            edition = pool.presentation_edition
            for contributor in edition.contributors:
                self.viaf_client.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())

    def resolve_cover_image(self, work):
        """Make sure we have the cover for all editions."""

        for pool in work.license_pools:
            edition = pool.presentation_edition
            data_source_name = pool.data_source.name
            if data_source_name in self.image_mirrors:
                self.image_mirrors[data_source_name].mirror_edition(edition)
                self.image_scaler.scale_edition(edition)
