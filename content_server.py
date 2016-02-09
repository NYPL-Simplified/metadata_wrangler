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

class ContentServerException(Exception):
    # Raised when the ContentServer can't connect or returns bad data
    pass

class ContentServerCoverageProvider(CoverageProvider):
    """Checks the OA Content Server for Records"""

    CONTENT_SERVER_RETURNED_ERROR = "OA Content Server returned error"
    CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE = "Content Server served \
            unhandleable media type"

    def __init__(self, _db):
        self._db = _db
        content_server_url = Configuration.integration_url(
            Configuration.CONTENT_SERVER_INTEGRATION, required=True)
        self.content_server = SimplifiedOPDSLookup(content_server_url)
        self.importer = OPDSImporter(self._db, DataSource.OA_CONTENT_SERVER)
        input_identifier_types = [Identifier.GUTENBERG_ID]
        output_source = DataSource.OA_CONTENT_SERVER
        super(ContentServerCoverageProvider, self).__init__(
                "OA Content Server Coverage Provider",
                input_identifier_types, output_source, workset_size=10
        )

    def process_item(self, identifier):
        response = self.content_server.lookup(identifier)
        self.check_response_for_errors(response)

        editions, messages = self.importer.import_from_feed(response)
        for edition in editions:
            # Check that this identifier's edition was imported
            # and return it as a success if so.
            edition_identifier = edition.primary_identifier
            if edition_identifier == identifier:
                return identifier
        for message_identifier, (status_code, exception) in messages.items():
            # Return messages as CoverageFailures.
            if message_identifier == identifier:
                if status_code == 200:
                    exception = "OA Content Server returned success, but \
                            nothing was imported"
                    return CoverageFailure(self, identifier, exception)
                return CoverageFailure(self, identifier, exception)

        exception = "404: Identifier %r was not found in %s" % (identifier,
                self.service_name)
        return CoverageFailure(self, identifier, exception)

    def check_response_for_errors(self, response):
        """Raises a server error if a response is not a success.

        TODO: This probably doesn't go here.
        """
        if response.status_code != 200:
            raise ContentServerException(self.CONTENT_SERVER_RETURNED_ERROR)

        content_type = response.headers['content-type']
        if not content_type.startswith("application/atom+xml"):
            raise ContentServerException(
                self.CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE % (
                content_type)
            )
