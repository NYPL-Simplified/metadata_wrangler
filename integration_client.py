from nose.tools import set_trace
from core.model import (
    CoverageRecord,
    DataSource,
    ExternalIntegration,
)
from core.coverage import CatalogCoverageProvider
from core.metadata_layer import (
    Metadata,
    ReplacementPolicy,
)

class IntegrationClientCoverageProvider(CatalogCoverageProvider):
    """Mirrors and scales cover images we heard about from an IntegrationClient."""

    SERVICE_NAME = "Integration Client Coverage Provider"
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING

    OPERATION = CoverageRecord.IMPORT_OPERATION
    PROTOCOL = ExternalIntegration.OPDS_FOR_DISTRIBUTORS

    def __init__(self, uploader, collection, *args, **kwargs):
        self.uploader = uploader
        super(IntegrationClientCoverageProvider, self).__init__(
            collection, *args, **kwargs)

    @property
    def data_source(self):
        """Use the collection's name as the data source name."""
        return DataSource.lookup(self._db, self.collection.name, autocreate=True)

    def process_item(self, identifier):
        edition = self.edition(identifier)
        replace = ReplacementPolicy(mirror=self.uploader, links=True)
        metadata = Metadata.from_edition(edition)
        metadata.apply(edition, self.collection, replace=replace)
        return identifier
