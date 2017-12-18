from nose.tools import set_trace
from sqlalchemy.orm.session import Session

from core.coverage import CollectionCoverageProvider
from core.model import DataSource
from core.metadata_layer import ReplacementPolicy
from core.overdrive import (
    OverdriveBibliographicCoverageProvider as BaseOverdriveBibliographicCoverageProvider
)
from core.s3 import S3Uploader

from mirror import CoverImageMirror

class OverdriveBibliographicCoverageProvider(
        BaseOverdriveBibliographicCoverageProvider):
    """Finds and updates bibliographic information for Overdrive items."""

    EXCLUDE_SEARCH_INDEX = True

    def __init__(self, collection, uploader=None, **kwargs):
        _db = Session.object_session(collection)
        self.mirror = uploader or S3Uploader.from_config(_db)
        kwargs['registered_only'] = True
        super(OverdriveBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )

    def _default_replacement_policy(self, _db):
        """Treat this as a trusted metadata source. Mirror any appropriate
        resources to S3.
        """
        return ReplacementPolicy.from_metadata_source(mirror=self.mirror)

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Finds the items that need coverage based on the collection's catalog
        instead of its license_pools. This is specific to work done on the
        Metadata Wrangler.

        TODO: Find a better way to combine Overdrive bibliographic coverage
        with catalog coverage. This approach represents a duplication of
        work in core.coverage.CatalogCoverageProvider.items_that_need_coverage.
        """
        qu = super(CollectionCoverageProvider, self).items_that_need_coverage(
            identifiers, **kwargs
        )
        qu = qu.join(Identifier.collections).filter(
            Collection.id==self.collection_id
        )
        return qu


class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
