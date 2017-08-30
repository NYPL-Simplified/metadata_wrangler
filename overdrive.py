from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from core.model import DataSource
from core.metadata_layer import ReplacementPolicy
from core.overdrive import (
    OverdriveBibliographicCoverageProvider as BaseOverdriveBibliographicCoverageProvider
)

from mirror import CoverImageMirror

class OverdriveBibliographicCoverageProvider(
        BaseOverdriveBibliographicCoverageProvider):
    """Finds and updates bibliographic information for Overdrive items."""

    EXCLUDE_SEARCH_INDEX = True

    def __init__(self, uploader, collection, *args, **kwargs):
        _db = Session.object_session(collection)
        self.mirror = uploader
        super(OverdriveBibliographicCoverageProvider, self).__init__(
            collection, *args, **kwargs
        )

    def _default_replacement_policy(self, _db):
        """Treat this as a trusted metadata source. Mirror any appropriate
        resources to S3.
        """
        return ReplacementPolicy.from_metadata_source(mirror=self.mirror)


class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
