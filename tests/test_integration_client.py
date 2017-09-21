from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from core.s3 import DummyS3Uploader

from . import (
    DatabaseTest,
)

from core.model import ExternalIntegration

from integration_client import (
    IntegrationClientCoverageProvider,
)

class TestIntegrationClientCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        uploader = DummyS3Uploader()
        collection = self._collection(
            protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        )

        provider = IntegrationClientCoverageProvider(
            uploader=uploader, collection=collection
        )
        eq_(collection.name, provider.data_source.name)
