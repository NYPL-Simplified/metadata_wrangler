from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from core.coverage import CoverageFailure
from core.s3 import DummyS3Uploader

from . import (
    DatabaseTest,
)
from integration_client import (
    IntegrationClientCoverageProvider,
)
from mirror import CoverImageMirror, ImageScaler

class TestIntegrationClientCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        uploader = DummyS3Uploader()
        provider = IntegrationClientCoverageProvider(
            uploader=uploader, collection=self._default_collection
        )
        assert isinstance(provider.mirror, CoverImageMirror)
        assert isinstance(provider.scaler, ImageScaler)
        eq_(self._default_collection.name, provider.mirror.DATA_SOURCE)

