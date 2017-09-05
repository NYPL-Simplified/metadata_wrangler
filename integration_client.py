from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from core.model import DataSource
from core.coverage import CollectionCoverageProvider
from mirror import CoverImageMirror, ImageScaler

class IntegrationClientCoverageProvider(CollectionCoverageProvider):
    """Mirrors and scales cover images we heard about from an IntegrationClient."""

    SERVICE_NAME = "Integration Client Coverage Provider"
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING

    def __init__(self, uploader, collection, *args, **kwargs):
        _db = Session.object_session(collection)
        data_source_name = collection.name
        data_source = DataSource.lookup(_db, data_source_name, autocreate=True)

        class IntegrationClientMirror(CoverImageMirror):
            DATA_SOURCE = data_source.name

        self.mirror = IntegrationClientMirror(_db, uploader=uploader)
        self.scaler = ImageScaler(_db, [self.mirror], uploader=uploader)

        super(IntegrationClientCoverageProvider, self).__init__(
            collection, *args, **kwargs)

    def process_item(self, identifier):
        edition = self.edition(identifier)
        self.mirror.mirror_edition(edition)
        self.scaler.scale_edition(edition)
        return identifier
