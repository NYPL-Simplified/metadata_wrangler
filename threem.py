from nose.tools import set_trace

from mirror import CoverImageMirror
from core.model import DataSource

class ThreeMCoverImageMirror(CoverImageMirror):
    """Downloads images from 3M and writes them to disk."""

    DATA_SOURCE = DataSource.THREEM

    def filename_for(self, resource):
        return resource.identifier.identifier + ".jpg"
