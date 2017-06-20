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
    IdentifierCoverageProvider,
    CoverageFailure,
)
from core.s3 import S3Uploader
from core.util.http import BadResponseException

from canonicalize import AuthorNameCanonicalizer

class ContentServerException(Exception):
    # Raised when the ContentServer can't connect or returns bad data
    pass

class ContentServerCoverageProvider(IdentifierCoverageProvider):
    """Checks the OA Content Server for metadata about Gutenberg books
    and books identified by URI.
    """

    SERVICE_NAME = u'OA Content Server Coverage Provider'

    DEFAULT_BATCH_SIZE = 10

    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER

    INPUT_IDENTIFIER_TYPES = [
        Identifier.GUTENBERG_ID,
        Identifier.URI
    ]

    CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE = "Content Server served unhandleable media type: %s"

    def __init__(self, _db, content_server=None, uploader=None,
                 metadata_client=None, **kwargs
    ):
        if not content_server:
            content_server_url = Configuration.integration_url(
                Configuration.CONTENT_SERVER_INTEGRATION, required=True)
            content_server = SimplifiedOPDSLookup(content_server_url)
        self.lookup = content_server

        collection = kwargs.get('collection')
        mirror = uploader or S3Uploader.from_config(_db)
        metadata_client = metadata_client or AuthorNameCanonicalizer(_db)
        self.importer = OPDSImporter(
            _db, collection, data_source_name=DataSource.OA_CONTENT_SERVER,
            mirror=mirror, metadata_client=metadata_client
        )

        super(ContentServerCoverageProvider, self).__init__(_db, **kwargs)

    def process_item(self, identifier):
        try:
            response = self.lookup.lookup([identifier])
        except BadResponseException, e:
            return CoverageFailure(
                identifier,
                e.message,
                self.data_source
            )
        content_type = response.headers['content-type']
        if not content_type.startswith("application/atom+xml"):
            return CoverageFailure(
                identifier,
                self.CONTENT_SERVER_RETURNED_WRONG_CONTENT_TYPE % (
                    content_type),
                self.data_source
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
            message = messages[0]
            # Because the Collection here is the Metadata Wrangler
            # default Collection
            message.data_source = self.data_source
            return message
        return CoverageFailure(
            identifier,
            "Identifier was not mentioned in lookup response",
            self.data_source
        )
