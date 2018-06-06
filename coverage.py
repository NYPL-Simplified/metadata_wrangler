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

from core.mirror import MirrorUploader

from core.util import fast_query_count

from overdrive import (
    OverdriveBibliographicCoverageProvider,
)

from content_cafe import (
    ContentCafeCoverageProvider, 
    ContentCafeAPI,
)

from content_server import (
    LookupClientCoverageProvider, 
)

from oclc_classify import (
    OCLCClassifyCoverageProvider, 
)

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
    CoverageProviders, which fill in the blanks with data from
    third-party entities.

    This CoverageProvider may force those other CoverageProviders to
    do their work for each Identifier immediately, or it may simply
    register its Identifiers with those CoverageProviders and allow
    them to complete the work at their own page.

    This CoverageProvider is invoked twice: once from the
    URNLookupController.process_urns, where its
    register_identifier_as_unresolved method or its ensure_coverage
    method may be called; and once from a script (TODO: which one?)
    which invokes it through RunCollectionCoverageProviderScript to
    ensure coverage for all previously registered identifiers.
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

    def __init__(self, collection, mirror=None, viaf_client=None, force=False,
                 **kwargs):
        """Constructor.

        :param collection: Handle all Identifiers from this Collection
        that were previously registered with this CoverageProvider.

        :param mirror: A MirrorUploader to use if coverage requires
        uploading any cover images to external storage.

        :param viaf_client: A VIAFClient to use if coverage requires
        gathering information about authors from VIAF.

        :param force: Force CoverageProviders to cover identifiers
        even if they believe they have already done the work.

        :param provide_coverage_immediately: If this is True, then
        resolving an identifier means registering it with all of its
        other CoverageProviders *and then attempting to provide
        coverage*.  Registration is considered a success even if the
        other CoverageProviders fail, but the attempt must be made
        immediately.

        If this is False (the default), then resolving an identifier
        just means registering it with all other relevant
        CoverageProviders.
        """
        self.provide_coverage_immediately = kwargs.pop(
            'provide_coverage_immediately', False
        )
        
        # We don't pass in registered_only=True because if an
        # Identifier is part of this collection's catalog it means
        # someone asked about it.
        super(IdentifierResolutionCoverageProvider, self).__init__(
            collection, **kwargs
        )

        # Since we are the metadata wrangler, any resources we find,
        # we mirror using the sitewide MirrorUploader.
        mirror = mirror or MirrorUploader.sitewide(self._db)
        self.mirror = mirror

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        
        # TODO: This replacement policy is what needs to be passed in to
        # the CoverageProviders, not just the mirror.
        #
        # TODO: Maybe the replacement policy should also contain the
        # VIAF client, since that needs to be used when the work is
        # finalized.
        self.policy = PresentationCalculationPolicy(
            regenerate_opds_entries=True, mirror=self.mirror
        )
        
        # Instantiate the coverage providers that may be needed to
        # relevant to any given Identifier.
        #
        # Each Identifier in this Collection's catalog will be registered
        # with all relevant providers (if provide_coverage_immediately
        # is False) or immediately covered by all relevant providers
        # (if provide_coverage_immediately is True).
        self.providers = self.providers()

    @classmethod
    def unaffiliated_collection(cls, _db):
        """Find a special metadata-wrangler-specific Collection whose catalog
        contains identifiers that came in through anonymous lookup.
        """
        return Collection.by_name_and_protocol(
            _db, "Unaffiliated Identifiers", DataSource.INTERNAL_PROCESSING
        )

    @classmethod
    def collections(cls, _db):
        """Return a list of covered collections. The 'unaffiliated' collection
        is always last in the list.
        """
        unaffiliated, ignore = cls.unaffiliated_collection(_db)
        collections = super(cls, cls).collections(_db)

        if unaffiliated in collections[:]:
            # Always put the unaffiliated collection last.
            collections.remove(unaffiliated)
            collections.append(unaffiliated)

        return collections

    def providers(self, provider_kwargs=None):
        """Instantiate all CoverageProviders that might be necessary
        to handle an Identifier from this Collection.

        All Identifiers in this Collection's catalog will be run
        through each provider that can handle its Identifier.type.

        :param provider_kwargs: A dictionary mapping
        CoverageProvider classes to dictionaries of keyword arguments
        to be used in those classes constructors. Used in testing to
        avoid creating CoverageProviders that make requests against
        real servers on instantiation.
        """

        def instantiate(self, cls, add_to, provider_kwargs, **kwargs):
            """Instantiate a CoverageProvider, possibly with mocked
            arguments, and add it to a list.

            :param cls: Instantiate this class.
            :param add_to: Add it to this list.
            :param provider_kwargs: Keyword arguments provided by
            test code to override the defaults.
            """
            # The testing setup may want us to instantiate a different
            # class entirely.
            cls = kwargs.pop('cls', cls)

            # The testing setup may want us to use different constructor
            # arguments than the default.
            this_provider_kwargs = provider_kwargs.get(cls)
            kwargs.update(this_provider_kwargs)

            add_to.append(cls(**kwargs))

        protocol = self.collection.protocol
        providers = []

        # These CoverageProviders can handle items from any kind of
        # collection, so long as the Identifier is of the right type.
        oclc_classify = instantiate(
            OCLCClassifyCoverageProvider, providers, provider_kwargs,
            _db=self._db
        )
        
        content_cafe = instantiate(
            ContentCafeCoverageProvider, providers, provider_kwargs,
            collection=self.collection, mirror=self.mirror
        )
            
        # All books derived from OPDS import must be looked up from the
        # server they were imported from.
        #
        # TODO: This is temporarily disabled, possibly permanently. It
        # doesn't work very well and if we really need this
        # information the circulation manager can provide it to us.
        #
        #if (protocol==ExternalIntegration.OPDS_IMPORT
        #    and self.collection.data_source
        #):
        #    opds_lookup = instantiate(
        #        LookupClientCoverageProvider, providers, provider_kwargs,
        #        collection=self.collection
        #    )

        # All books obtained from Overdrive must be looked up via the
        # Overdrive API.
        if protocol == ExternalIntegration.OVERDRIVE:
            overdrive = instantiate(
                OverdriveBibliographicCoverageProvider, providers,
                provider_kwargs, collection=self.collection,
                mirror=self.mirror
            )

        # We already have metadata for books we heard about from an
        # IntegrationClient, but we need to make sure the covers get
        # mirrored.
        if protocol == ExternalIntegration.OPDS_FOR_DISTRIBUTORS:
            instantiate(
                IntegrationClientCoverImageCoverageProvider, providers,
                provider_kwargs, collection=self.collection, mirror=self.mirror,
            )

        return providers
            
    def process_item(self, identifier):
        """Either make sure this Identifier is registered with all
        CoverageProviders, or actually attempt to use them to provide
        all overage.        
        """
        if self.provide_coverage_immediately:
            message = "Immediately providing coverage for %s"
        else:
            message = "Registering %s with coverage providers"
        self.log.info(message, identifier)

        # Make sure there's a LicensePool for this Identifier in this
        # Collection. Since we're the metadata wrangler, the
        # LicensePool is a stub that doesn't actually represent the
        # right to loan the book, but that's okay.
        license_pool = self.license_pool(identifier)
        if not license_pool.licenses_owned:
            license_pool.update_availability(1, 1, 0, 0)

        for provider in self.providers:
            self.process_one_provider(identifier, provider)

        # The only way this can fail is if there is an uncaught exception
        # during the registration/processing process. The failure of a
        # CoverageProvider to provide coverage doesn't mean this process
        # has failed -- that's a problem that the CoverageProvider itself
        # can resolve later.
        return identifier

    def process_one_provider(self, identifier, provider):
        if not provider.can_cover(identifier):
            # The CoverageProvider under consideration doesn't
            # handle Identifiers of this type.
            return

        if provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            # We need to cover this Identifier once, and then we're
            # done, for all collections.
            collection = None
        else:
            # We need separate coverage for this Collection
            # specifically.
            collection = self.collection

        if self.provide_coverage_immediately:
            # TODO: ensure_coverage needs to take a collection argument.
            provider.ensure_coverage(identifier, force=self.force)
        else:
            provider.register(
                identifier, collection=collection, force=self.force
            )

    def resolve_viaf(self, work):
        """Get VIAF data on all contributors.

        TODO: This needs to be in a mix-in class which is used as a
        post-coverage hook by all CoverageProviders that might add contributors
        to a work.
        """

        for pool in work.license_pools:
            edition = pool.presentation_edition
            if not edition:
                continue
            for contributor in edition.contributors:
                self.viaf_client.process_contributor(contributor)
                if not contributor.display_name:
                    contributor.family_name, contributor.display_name = (
                        contributor.default_names())
