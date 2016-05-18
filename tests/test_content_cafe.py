from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider,
)
from core.s3 import DummyS3Uploader

from . import (
    DatabaseTest,
)

class DummyContentCafeAPI(object):
    pass

class DummyContentCafeSOAPClient(object):
    pass

class TestContentCafeCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        uploader=DummyS3Uploader()
        soap_client = DummyContentCafeSOAPClient()
        api = ContentCafeAPI(self._db, None, "user_id", "password", 
                             uploader, soap_client=soap_client)
        provider = ContentCafeCoverageProvider(
            self._db, api=api, uploader=uploader
        )
