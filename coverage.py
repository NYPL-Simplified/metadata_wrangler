import logging
from nose.tools import set_trace

from sqlalchemy.orm.session import Session

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
    IntegrationClientCoverImageCoverageProvider,
)


class IdentifierResolutionCoverageProvider(CatalogCoverageProvider):
    """Make sure all Identifiers associated with some Collection become
    Works.

    Coverage happens by running the Identifier through _other_
    CoverageProviders, which fill in the blanks with data from
    third-party entities.

    This CoverageProvider may force those other CoverageProviders to
    do their work for each Identifier immediately, or it may simply
    register its Identifiers with those CoverageProviders and allow
    them to complete the work at their own pace.

    Unlike most CoverageProviders, which are invoked from a script,
    this CoverageProvider is invoked from
    URNLookupController.process_urns, and only when a client expresses
    a desire that we look into a specific identifier.
    """

    SERVICE_NAME = "Identifier Resolution Coverage Provider"
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING

    # These are the only identifier types we have any hope of providing
    # insight into.
    INPUT_IDENTIFIER_TYPES = [
        Identifier.OVERDRIVE_ID, Identifier.ISBN, Identifier.URI,
    ]
    OPERATION = CoverageRecord.RESOLVE_IDENTIFIER_OPERATION

    # We cover all Collections, regardless of their protocol.
    PROTOCOL = None

    def __init__(self, collection, mirror=None, http_get=None, viaf=None,
                 provide_coverage_immediately=False, force=False,
                 provider_kwargs=None, **kwargs
    ):
        """Constructor.

        :param collection: Handle all Identifiers from this Collection
        that were previously registered with this CoverageProvider.

        :param mirror: A MirrorUploader to use if coverage requires
        uploading any cover images to external storage.

        :param http_get: A drop-in replacement for
        Representation.simple_http_get, to be used if any information
        (such as a book cover) needs to be obtained from the public
        Internet.

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

        :param provider_kwargs: Pass this object in as provider_kwargs
        when calling gather_providers at the end of the
        constructor. Used only in testing.

        """
        _db = Session.object_session(collection)

        # Since we are the metadata wrangler, any resources we find,
        # we mirror using the sitewide MirrorUploader.
        mirror = mirror or MirrorUploader.sitewide(_db)
        self.mirror = mirror

        # We're going to be aggressive about recalculating the presentation
        # for this work because either the work is currently not set up
        # at all, or something went wrong trying to set it up.
        presentation = PresentationCalculationPolicy(
            regenerate_opds_entries=True
        )
        replacement_policy = ReplacementPolicy.from_metadata_source(
            presentation_calculation_policy=presentation, mirror=self.mirror,
            http_get=http_get,
        )
        super(IdentifierResolutionCoverageProvider, self).__init__(
            collection, replacement_policy=replacement_policy,
            **kwargs
        )

        self.provide_coverage_immediately = provide_coverage_immediately
        self.force = force

        self.viaf = viaf or VIAFClient(self._db)

        # Instantiate the coverage providers that may be needed to
        # relevant to any given Identifier.
        #
        # Each Identifier in this Collection's catalog will be registered
        # with all relevant providers (if provide_coverage_immediately
        # is False) or immediately covered by all relevant providers
        # (if provide_coverage_immediately is True).
        self.providers = self.gather_providers(provider_kwargs)

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
        collections = super(
            IdentifierResolutionCoverageProvider, cls).collections(_db)

        if unaffiliated in collections[:]:
            # Always put the unaffiliated collection last.
            collections.remove(unaffiliated)
            collections.append(unaffiliated)

        return collections

    def gather_providers(self, provider_kwargs=None):
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

        def instantiate(cls, add_to, provider_kwargs, **kwargs):
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
            provider_kwargs = provider_kwargs or {}
            this_provider_kwargs = provider_kwargs.get(cls, {})
            kwargs.update(this_provider_kwargs)

            add_to.append(cls(**kwargs))

        protocol = self.collection.protocol
        providers = []

        # These CoverageProviders can handle items from any kind of
        # collection, so long as the Identifier is of the right type.

        # TODO: This is temporarily disabled -- it needs to become
        # a CollectionCoverageProvider.
        #
        # There's no rush to get this working again because
        # it was primarily intended for use with Project Gutenberg titles,
        # which we've downplayed in favor of Feedbooks titles, which
        # have much better metadata.
        #
        #oclc_classify = instantiate(
        #    OCLCClassifyCoverageProvider, providers, provider_kwargs,
        #    _db=self._db
        #)

        content_cafe = instantiate(
            ContentCafeCoverageProvider, providers, provider_kwargs,
            collection=self.collection,
            replacement_policy=self.replacement_policy
        )

        # TODO: This is temporarily disabled because its process_item doesn't
        # work directly on ISBNs -- it assumes the ISBN has already been
        # associated with OCLC Numbers in some other step. The best
        # solution is to rearchitect LinkedDataCoverageProvider
        # to make it assume it's processing ISBNs.
        #
        # This is fine for now because the main things we need out of the
        # metadata wrangler are cover images and descriptions, which
        # we can get from ContentCafeCoverageProvider.
        #
        #linked_data = instantiate(
        #    LinkedDataCoverageProvider, providers, provider_kwargs,
        #    collection=self.collection, replacement_policy=self.policy,
        #    viaf=self.viaf
        #)

        # All books identified by Overdrive ID must be looked up via
        # the Overdrive API. We don't enforce that the collection
        # is an Overdrive collection, because we want to allow
        # unauthenticated lookups in the 'unaffiliated' collection.
        try:
            overdrive = instantiate(
                OverdriveBibliographicCoverageProvider, providers,
                provider_kwargs, collection=self.collection,
                viaf=self.viaf, replacement_policy=self.replacement_policy
            )
        except CannotLoadConfiguration, e:
            # No Overdrive collection is configured -- we can't
            # handle Overdrive lookups, as much as we'd like to.
            pass

        # We already have metadata for books we heard about from an
        # IntegrationClient, but we need to make sure the covers get
        # mirrored.
        if protocol == ExternalIntegration.OPDS_FOR_DISTRIBUTORS:
            instantiate(
                IntegrationClientCoverImageCoverageProvider, providers,
                provider_kwargs, collection=self.collection,
                replacement_policy=self.replacement_policy
            )

        return providers

    def process_item(self, identifier):
        """Either make sure this Identifier is registered with all
        CoverageProviders, or actually attempt to use them to provide
        all coverage.
        """
        if self.provide_coverage_immediately:
            message = "Immediately providing coverage for %s."
        else:
            message = "Registering %s with coverage providers."
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

        # TODO: This code could be moved into
        # IdentifierCoverageProvider.register, if it weren't a class
        # method. This would simplify testing.
        if provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION:
            # We need to cover this Identifier once, and then we're
            # done, for all collections.
            collection = None
        else:
            # We need separate coverage for the specific Collection
            # associated with this CoverageProvider.
            collection = provider.collection

        if self.provide_coverage_immediately:
            provider.ensure_coverage(identifier, force=self.force)
        else:
            provider.register(
                identifier, collection=collection, force=self.force
            )
