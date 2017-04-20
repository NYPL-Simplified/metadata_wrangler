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

class ContentServerException(Exception):
    # Raised when the ContentServer can't connect or returns bad data
    pass

class ContentServerCoverageProvider(CatalogCoverageProvider):
    """Checks the OA Content Server for metadata about Gutenberg books
    and books identified by URI.
    """

    SERVICE_NAME = "OA Content Server Coverage Provider"
    PROTOCOL = Collection.OPDS_IMPORT
    INPUT_IDENTIFIER_TYPES = [Identifier.GUTENBERG_ID, Identifier.URI]
    
    CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE = "Content Server served unhandleable media type: %s"
    
    def __init__(self, collection, lookup_client=None,
                 importer=None, **kwargs):
        self.DATA_SOURCE_NAME = collection.data_source.name
        super(ContentServerCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if not lookup_client:
            # TODO: It should be possible to get this information
            # from the Collection object.
            content_server_url = Configuration.integration_url(
                Configuration.CONTENT_SERVER_INTEGRATION, required=True
            )
            lookup_client = SimplifiedOPDSLookup(content_server_url)
        self.lookup_client = lookup_client
        if not importer:
            importer = OPDSImporter(
                self._db, collection, collection.data_source.name
            )
        self.importer = importer
        
    def process_item(self, identifier):
        try:
            response = self.lookup_client.lookup([identifier])
        except BadResponseException, e:
            return self.failure(identifier, e.message)
        content_type = response.headers['content-type']
        if not content_type.startswith("application/atom+xml"):
            return self.failure(
                identifier,
                self.CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE % (
                    content_type
                )
            )
        
        editions, licensepools, works, messages = self.importer.import_from_feed(
            response.content
        )
        for edition in editions:
            # Check that this identifier's edition was imported
            # and return it as a success if so.
            edition_identifier = edition.primary_identifier
            if edition_identifier == identifier:
                return identifier
        expect = identifier.urn
        messages = messages.values()
        if messages:
            return messages[0]
        return self.failure(
            identifier, "Identifier was not mentioned in lookup response"
        )
