from nose.tools import set_trace, eq_

from core.s3 import DummyS3Uploader
from core.coverage import CoverageFailure

from . import (
    DatabaseTest,
)
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider,
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

    def test_process_item_can_return_coverage_failure(self):

        class AlwaysFailsContentCafe(DummyContentCafeAPI):
            mirror = None
            def mirror_resources(self, identifier):
                raise Exception("Oh no!")

        provider = ContentCafeCoverageProvider(
            self._db, api=AlwaysFailsContentCafe(), uploader=DummyS3Uploader()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        assert "Oh no!" in result.exception
