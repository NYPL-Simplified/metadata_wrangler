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
from core.coverage import IdentifierCoverageProvider
from core.util.http import BadResponseException

class ContentServerException(Exception):
    # Raised when the ContentServer can't connect or returns bad data
    pass

class ContentServerCoverageProvider(IdentifierCoverageProvider):
    """Checks the OA Content Server for metadata about Gutenberg books
    and books identified by URI.

    Although this uses an OPDSImporter, it is not a
    CollectionCoverageProvider because it does not aim to create 
    LicensePools in any particular Collection, only Editions.
    """

    SERVICE_NAME = "OA Content Server Coverage Provider"
    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER
    INPUT_IDENTIFIER_TYPES = [Identifier.GUTENBERG_ID, Identifier.URI]
    
    CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE = "Content Server served unhandleable media type: %s"
    
    def __init__(self, _db, lookup_client=None,
                 importer=None, **kwargs):
        super(ContentServerCoverageProvider, self).__init__(
            _db, **kwargs
        )
        if not lookup_client:
            content_server_url = Configuration.integration_url(
                Configuration.CONTENT_SERVER_INTEGRATION, required=True
            )
            lookup_client = SimplifiedOPDSLookup(content_server_url)
        self.lookup_client = lookup_client
        if not importer:
            importer = OPDSImporter(
                self._db, self.collection, self.DATA_SOURCE_NAME
            )
        self.importer = importer

    def process_item(self, identifier):
        data_source = self.collection.data_source
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
