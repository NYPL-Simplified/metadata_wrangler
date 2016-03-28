from nose.tools import set_trace

from mirror import (
    CoverImageMirror
)
from core.model import DataSource

class OverdriveCoverImageMirror(CoverImageMirror):
    """Downloads images from Overdrive and writes them to disk."""

    DATA_SOURCE = DataSource.OVERDRIVE
