from nose.tools import set_trace
from core.config import Configuration
from core.model import (
    DataSource,
    Identifier,
)
from core.opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
)
from core.coverage import (
    CoverageProvider,
    CoverageFailure,
)
from core.util.http import BadResponseException

class ContentServerException(Exception):
    # Raised when the ContentServer can't connect or returns bad data
    pass

class ContentServerCoverageProvider(CoverageProvider):
    """Checks the OA Content Server for Records"""

    CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE = "Content Server served unhandleable media type: %s"

    def __init__(self, _db, content_server=None):
        self._db = _db
        if not content_server:
            content_server_url = Configuration.integration_url(
                Configuration.CONTENT_SERVER_INTEGRATION, required=True)
            content_server = SimplifiedOPDSLookup(content_server_url)
        self.content_server = content_server
        self.importer = OPDSImporter(self._db, DataSource.OA_CONTENT_SERVER)
        input_identifier_types = [Identifier.GUTENBERG_ID, Identifier.URI]
        output_source = DataSource.lookup(
            self._db, DataSource.OA_CONTENT_SERVER
        )
        super(ContentServerCoverageProvider, self).__init__(
                "OA Content Server Coverage Provider",
                input_identifier_types, output_source, batch_size=10
        )

    def process_item(self, identifier):
        data_source = DataSource.lookup(
            self._db, self.importer.data_source_name
        )
        try:
            response = self.content_server.lookup([identifier])
        except BadResponseException, e:
            return CoverageFailure(
                identifier,
                e.message,
                data_source
            )
        content_type = response.headers['content-type']
        if not content_type.startswith("application/atom+xml"):
            return CoverageFailure(
                identifier,
                self.CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE % (
                    content_type)
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
        return CoverageFailure(
            identifier,
            "Identifier was not mentioned in lookup response",
            data_source
        )
