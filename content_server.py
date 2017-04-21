import urlparse
from nose.tools import set_trace
from core.config import Configuration
from core.model import (
    Collection,
    DataSource,
    Identifier,
)
from core.opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
)
from core.coverage import CatalogCoverageProvider
from core.util.http import BadResponseException


class LookupClientCoverageProvider(CatalogCoverageProvider):
    """Uses the Library Simplified OPDS Lookup Protocol to get
    extra information about books in a Catalog.
    """

    # TODO: We should rename this because in theory it can be used
    # other places, but in practice this is it.
    SERVICE_NAME = "OA Content Server Coverage Provider"
    PROTOCOL = Collection.OPDS_IMPORT
    
    OPDS_SERVER_RETURNED_WRONG_CONTENT_TYPE = "OPDS Server served unhandleable media type: %s"
   
    def __init__(self, collection, **kwargs):
        self.DATA_SOURCE_NAME = collection.data_source.name
        super(LookupClientCoverageProvider, self).__init__(
            collection, **kwargs
        )

        # Assume that this collection's OPDS server also implements
        # the lookup protocol.
        feed_url = collection.external_account_id
        root = urlparse.urljoin(feed_url, '/')
        self.lookup_client = self._lookup_client(root)
        self.importer = self._importer()

    def _lookup_client(self, root):
        return SimplifiedOPDSLookup(root)
        
    def _importer(self):
        """Instantiate an appropriate OPDSImporter for the given Collection."""
        collection = self.collection
        return OPDSImporter(self._db, collection, collection.data_source.name)
        
    def process_item(self, identifier):
        try:
            response = self.lookup_client.lookup([identifier])
        except BadResponseException, e:
            return self.failure(identifier, e.message)
        content_type = response.headers.get('content-type')
        if not content_type or not content_type.startswith(
                "application/atom+xml"
        ):
            return self.failure(
                identifier,
                self.OPDS_SERVER_RETURNED_WRONG_CONTENT_TYPE % (
                    content_type
                )
            )
        
        editions, licensepools, works, messages = self.importer.import_from_feed(
            response.content
        )
        for edition in editions:
            # If an Edition for this identifier was imported, return
            # the Identifier to indicate success.
            edition_identifier = edition.primary_identifier
            if edition_identifier == identifier:
                return identifier
        expect = identifier.urn
        messages = messages.values()
        if messages:
            # OPDSImporter turns <simplified:message: tags into
            # CoverageFailures, which can be returned directly.
            return messages[0]
        return self.failure(
            identifier, "Identifier was not mentioned in lookup response"
        )
