import logging
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
    IntegrationClientCoverageProvider,
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
            collection, preregistered_only=True, **kwargs
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
        if is_new:
            raise ValueError('Default Overdrive collection has not been configured.')
        return overdrive_api_class(self._db, collection)

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
        if self.collection.protocol == ExternalIntegration.OVERDRIVE:
            required.append(
                OverdriveBibliographicCoverageProvider(
                    self.uploader, self.collection, api_class=self.overdrive_api
                )
            )

        # We already have metadata for books we heard about from an
        # IntegrationClient, but we need to make sure the covers get
        # mirrored.
        if self.collection.protocol == ExternalIntegration.OPDS_FOR_DISTRIBUTORS:
            required.append(
                IntegrationClientCoverageProvider(
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

        if identifier.type==Identifier.ISBN:
            # In order to create Works for ISBNs, we first have to
            # create an edition associated with the ISBN as a primary
            # identifier. At the moment, this is achieved via OCLC
            # Linked Data.
            self.generate_edition(identifier)
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
            work, created = pool.calculate_work(
                even_if_no_author=True, exclude_search=True
            )
        if work:
            self.resolve_viaf(work)

            work.calculate_presentation(
                policy=self.policy, exclude_search=True,
                default_fiction=None, default_audience=None,
            )
            work.set_presentation_ready(exclude_search=True)
        else:
            error_msg = "500; " + "Work could not be calculated for %r" % identifier
            raise RuntimeError(error_msg)

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


class IdentifierResolutionRegistrar(object):

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
        IntegrationClientCoverageProvider,
        LookupClientCoverageProvider,
    ]

    NO_WORK_DONE_EXCEPTION = u'No work done yet'

    def __init__(self, _db):
        self._db = _db

    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.__class__.__name__)
        return self._log

    def register(self, identifier, force=False):
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

        record = self.resolution_coverage(identifier)
        if record and not force:
            return record, False

        self.log.info('Identifying required coverage for %r' % identifier)
        providers = list()

        # Every identifier gets the resolver.
        providers.append(self.RESOLVER)

        # Filter Identifier-typed CoverageProviders.
        for provider in self.IDENTIFIER_PROVIDERS:
            if (not provider.INPUT_IDENTIFIER_TYPES
                or identifier.type in provider.INPUT_IDENTIFIER_TYPES
            ):
                providers.append(provider)

        for provider in self.COLLECTION_PROVIDERS:
            if not provider.PROTOCOL:
                providers.append(provider)
                continue

            covered_collections = filter(
                lambda c: c.protocol==provider.PROTOCOL, identifier.collections
            )
            if covered_collections:
                if provider==LookupClientCoverageProvider:
                    # The LookupClientCoverageProvider doesn't have an obvious
                    # data source. It uses the collection's data source instead.
                    for collection in covered_collections:
                        self.find_or_create_coverage_record(
                            identifier, provider, collection=collection
                        )
                else:
                    providers.append(provider)

        for provider_class in providers:
            self.find_or_create_coverage_record(identifier, provider_class)

        record = self.resolution_coverage(identifier)
        return record, True

    def resolution_coverage(self, identifier):
        """Returns a CoverageRecord if the given identifier has been registered
        for resolution with the IdentifierResolutionCoverageProvider

        :return: CoverageRecord or None
        """
        source = DataSource.lookup(self._db, self.RESOLVER.DATA_SOURCE_NAME)
        operation = self.RESOLVER.OPERATION
        return CoverageRecord.lookup(identifier, source, operation)

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

    def find_or_create_coverage_record(self, identifier, provider_class,
        collection=None
    ):
        source = DataSource.lookup(self._db, provider_class.DATA_SOURCE_NAME)
        if collection and not source:
            # LookupClientCoverageProvider has a DataSource specific to the
            # Collection, so OPDS_IMPORT collections should have their
            # DataSource set via request.
            source = collection.data_source
        operation = provider_class.OPERATION

        existing_record = CoverageRecord.lookup(identifier, source, operation)
        if existing_record:
            self.log.info(
                '[%s] FOUND %r' % (provider_class.__name__, existing_record)
            )
            return

        if not existing_record:
            coverage_record, is_new = CoverageRecord.add_for(
                identifier, source, operation=operation,
                status=CoverageRecord.TRANSIENT_FAILURE
            )
            coverage_record.exception = self.NO_WORK_DONE_EXCEPTION

            self.log.info(
                '[%s] CREATED %r' % (provider_class.__name__, coverage_record)
            )
