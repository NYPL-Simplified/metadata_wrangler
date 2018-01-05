import logging
from nose.tools import set_trace

from core.config import CannotLoadConfiguration

from core.coverage import (
    CoverageFailure, 
    CatalogCoverageProvider, 
    IdentifierCoverageProvider,
)

from core.metadata_layer import (
    ReplacementPolicy, 
)

from core.model import (
    Collection,
    ConfigurationSetting,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    LicensePool,
    PresentationCalculationPolicy,
    get_one_or_create,
)

from core.overdrive import OverdriveAPI

from core.s3 import (
    S3Uploader, 
)

from core.util import fast_query_count

from overdrive import (
    OverdriveBibliographicCoverageProvider,
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
from integration_client import (
    CalculatesWorkPresentation,
    IntegrationClientCoverImageCoverageProvider,
)


class IdentifierResolutionCoverageProvider(CatalogCoverageProvider,
    CalculatesWorkPresentation
):
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
    INPUT_IDENTIFIER_TYPES = [
        Identifier.OVERDRIVE_ID, Identifier.ISBN, Identifier.URI,
        Identifier.GUTENBERG_ID
    ]
    OPERATION = CoverageRecord.RESOLVE_IDENTIFIER_OPERATION
    
    LICENSE_SOURCE_NOT_ACCESSIBLE = (
        "Could not access underlying license source over the network.")
    UNKNOWN_FAILURE = "Unknown failure."

    DEFAULT_OVERDRIVE_COLLECTION_NAME = u'Default Overdrive'

    # We cover all Collections, regardless of their protocol.
    PROTOCOL = None

    def __init__(
        self, collection, uploader=None, viaf_client=None,
        content_cafe_api=None, overdrive_api_class=OverdriveAPI, **kwargs
    ):

        super(IdentifierResolutionCoverageProvider, self).__init__(
            collection, registered_only=True, **kwargs
        )

        # Since we are the metadata wrangler, any resources we find,
        # we mirror to S3.
        if not uploader:
            uploader = S3Uploader.from_config(self._db)
        self.uploader = uploader

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        self.policy = PresentationCalculationPolicy(
            regenerate_opds_entries=True
        )

        self.overdrive_api = self.create_overdrive_api(overdrive_api_class)

        self.content_cafe_api = content_cafe_api
        
        # Determine the optional and required coverage providers.
        # Each Identifier in this Collection's catalog will be run
        # through all relevant providers.
        self.required_coverage_providers, self.optional_coverage_providers = self.providers()

        # When we need to look up a contributor via VIAF we will use this
        # client.
        self.viaf_client = viaf_client or VIAFClient(self._db)

    def create_overdrive_api(self, overdrive_api_class):
        collection, is_new = Collection.by_name_and_protocol(
            self._db, self.DEFAULT_OVERDRIVE_COLLECTION_NAME,
            ExternalIntegration.OVERDRIVE
        )
        try:
            return overdrive_api_class(self._db, collection)
        except CannotLoadConfiguration, e:
            self.log.error(
                'Default Overdrive collection is not properly configured. No Overdrive work will be done.'
            )
            return

    @classmethod
    def unaffiliated_collection(cls, _db):
        """Find a special metadata-wrangler-specific Collection whose catalog
        contains identifiers that came in through anonymous lookup.
        """
        return Collection.by_name_and_protocol(
            _db, "Unaffiliated Identifiers", DataSource.INTERNAL_PROCESSING
        )

    @classmethod
    def all(cls, _db, **kwargs):
        """Yield a sequence of IdentifierResolutionCoverageProvider instances,
        one for every collection.

        The 'unaffiliated' collection is always last in the list.
        """
        unaffiliated, ignore = cls.unaffiliated_collection(_db)
        instances = list(super(IdentifierResolutionCoverageProvider, cls).all(
            _db, **kwargs
        ))
        collections = [x.collection for x in instances]
        if unaffiliated in collections:
            i = collections.index(unaffiliated)
            unaffiliated_instance = instances[i]
            instances.remove(unaffiliated_instance)
            instances.append(unaffiliated_instance)
        return instances

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

        if self.collection.protocol == ExternalIntegration.OPDS_FOR_DISTRIBUTORS:
            # If a book came from an OPDS for distributors collection, it may
            # not have an identifier that can be looked up elsewhere.
            optional = [content_cafe, oclc_classify]
            required = []
        else:
            optional = []
            required = [content_cafe, oclc_classify]
            
        # All books derived from OPDS import against an open-access
        # content server must be looked up in that server.
        if (self.collection.protocol==ExternalIntegration.OPDS_IMPORT
            and self.collection.data_source
        ):
            required.append(LookupClientCoverageProvider(self.collection))

        # All books obtained from Overdrive must be looked up via the
        # Overdrive API.
        if self.overdrive_api and self.collection.protocol == ExternalIntegration.OVERDRIVE:
            required.append(
                OverdriveBibliographicCoverageProvider(
                    self.collection, uploader=self.uploader,
                    api_class=self.overdrive_api
                )
            )

        # We already have metadata for books we heard about from an
        # IntegrationClient, but we need to make sure the covers get
        # mirrored.
        if self.collection.protocol == ExternalIntegration.OPDS_FOR_DISTRIBUTORS:
            required.append(
                IntegrationClientCoverImageCoverageProvider(
                    self.uploader, self.collection
                )
            )

        return required, optional
            
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
        if not license_pool.licenses_owned:
            license_pool.update_availability(1, 1, 0, 0)

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
        # and none of the optional coverage providers raised an exception.
        #
        # Register the identifier's work for presentation calculation.
        failure = self.register_work_for_calculation(identifier)
        if failure:
            return failure

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

    def presentation_calculation_pre_hook(self, work):
        """A hook method for the CalculatesWorkPresentation mixin"""
        self.resolve_viaf(work)

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors."""

        for pool in work.license_pools:
            edition = pool.presentation_edition
            if not edition:
                continue
            for contributor in edition.contributors:
                self.viaf_client.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())


class IdentifierResolutionRegistrar(CatalogCoverageProvider):

    # All of the providers used to resolve an Identifier for the
    # Metadata Wrangler.
    RESOLVER = IdentifierResolutionCoverageProvider

    IDENTIFIER_PROVIDERS = [
        ContentCafeCoverageProvider,
        LinkedDataCoverageProvider,
        OCLCClassifyCoverageProvider,
        OverdriveBibliographicCoverageProvider,
    ]

    COLLECTION_PROVIDERS = [
        IntegrationClientCoverImageCoverageProvider,
        LookupClientCoverageProvider,
    ]

    SERVICE_NAME = 'Identifier Resolution Registrar'
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING
    OPERATION = 'register-for-metadata'

    # Any kind of identifier can be registered.
    INPUT_IDENTIFIER_TYPES = None

    # An identifier can be catalogued with any type of Collection.
    PROTOCOL = None

    def process_item(self, identifier, force=False):
        """Creates a transient failure CoverageRecord for each provider
        that the identifier eligible for coverage from.

        :return: (CoverageRecord, bool) tuple with a CoverageRecord
        for the IdentifierResolutionCoverageProvider and a boolean representing
        whether or not the CoverageRecord is new
        """
        collection, ignore = self.RESOLVER.unaffiliated_collection(self._db)
        if not identifier.collections:
            # This Identifier is not in any collections. Add it to the
            # 'unaffiliated' collection to make sure it gets covered
            # eventually by the identifier resolution script, which only
            # covers Identifiers that are in some collection.
            collection.catalog.append(identifier)

        # Give the identifier a mock LicensePool if it doesn't have one.
        self.license_pool(identifier, collection)

        self.log.info('Identifying required coverage for %r' % identifier)

        # Get Collection coverage before resolution coverage, to make sure
        # that an identifier that's been added to a new collection is
        # registered for any relevant coverage -- even if its resolution has
        # already been completed.
        #
        # This is extremely important for coverage providers with
        # COVERAGE_COUNTS_FOR_EVERY_COLLECTION set to False.
        providers = self.collection_coverage_providers(identifier)

        # Find an resolution CoverageRecord if it exists.
        resolution_record = self.resolution_coverage(identifier)
        if resolution_record and not force:
            return identifier

        # Every identifier gets the resolver.
        providers.append(self.RESOLVER)

        # Filter Identifier-typed CoverageProviders.
        for provider in self.IDENTIFIER_PROVIDERS:
            if (not provider.INPUT_IDENTIFIER_TYPES
                or identifier.type in provider.INPUT_IDENTIFIER_TYPES
            ):
                providers.append(provider)

        for provider_class in providers:
            provider_class.register(identifier)

        return identifier

    @classmethod
    def resolution_coverage(cls, identifier):
        """Returns a CoverageRecord if the given identifier has been registered
        for resolution with the IdentifierResolutionCoverageProvider

        :return: CoverageRecord or None
        """
        source = cls.RESOLVER.DATA_SOURCE_NAME
        operation = cls.RESOLVER.OPERATION
        return CoverageRecord.lookup(identifier, source, operation)

    @classmethod
    def collection_coverage_providers(cls, identifier):
        """Determines the required catalog-based coverage an identifier needs.

        :return: A list of Collection- and CatalogCoverageProviders
        """
        providers = list()
        for provider in cls.COLLECTION_PROVIDERS:
            if not provider.PROTOCOL:
                providers.append(provider)
                continue

            covered_collections = filter(
                lambda c: c.protocol==provider.PROTOCOL, identifier.collections
            )

            if not covered_collections:
                continue

            is_lookup_client_provider = provider==LookupClientCoverageProvider
            if (is_lookup_client_provider or
                not provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION
            ):
                for collection in covered_collections:
                    data_source = None
                    if is_lookup_client_provider:
                        # The LookupClientCoverageProvider doesn't have an
                        # obvious data source. It uses the collection's data.
                        # source instead.
                        data_source = collection.data_source

                    _record, newly_registered = provider.register(
                        identifier, data_source=data_source,
                        collection=collection
                    )
            else:
                providers.append(provider)
        return providers

    def license_pool(self, identifier, collection):
        """Creates a LicensePool in the unaffiliated_collection for
        otherwise unlicensed identifiers.
        """
        license_pool = None
        if not identifier.licensed_through:
            license_pool, ignore = LicensePool.for_foreign_id(
                self._db, self.RESOLVER.DATA_SOURCE_NAME, identifier.type,
                identifier.identifier, collection=collection
            )

        license_pool = license_pool or identifier.licensed_through[0]

        if not license_pool.licenses_owned:
            license_pool.update_availability(1, 1, 0, 0)
