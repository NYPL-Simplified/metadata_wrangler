from nose.tools import set_trace
from core.model import DataSource
from core.overdrive import (
    OverdriveBibliographicCoverageProvider as BaseOverdriveBibliographicCoverageProvider
)

from mirror import CoverImageMirror

class OverdriveBibliographicCoverageProvider(BaseOverdriveBibliographicCoverageProvider):
    """Finds and updates bibliographic information for Overdrive items."""

    EXCLUDE_SEARCH_INDEX = True


class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
