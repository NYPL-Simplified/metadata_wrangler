from ..content_cafe import ContentCafeCoverageProvider

from . import (
    DatabaseTest,
)

class TestContentCafeCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        provider = ContentCafeCoverageProvider(self._db)
