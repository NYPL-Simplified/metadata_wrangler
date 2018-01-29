from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.model import ExternalIntegration
from core.s3 import MockS3Uploader

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

class TestContentCafeAPI(DatabaseTest):

    def test_from_config(self):
        # Without an integration, an error is raised.
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config,
            self._db, object()
        )

        # With incomplete integrations, an error is raised.
        integration = self._external_integration(
            ExternalIntegration.CONTENT_CAFE,
            goal=ExternalIntegration.METADATA_GOAL,
            username=u'yup'
        )
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config,
            self._db, object()
        )

        integration.username = None
        integration.password = u'yurp'
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config,
            self._db, object()
        )

        integration.username = u'yup'
        result = ContentCafeAPI.from_config(
            self._db, None, uploader=MockS3Uploader(),
            soap_client=DummyContentCafeSOAPClient()
        )
        eq_(True, isinstance(result, ContentCafeAPI))


class TestContentCafeCoverageProvider(DatabaseTest):

    def test_constructor(self):
        """Just test that we can create the object."""
        uploader=MockS3Uploader()
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
            self._db, api=AlwaysFailsContentCafe(), uploader=MockS3Uploader()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        assert "Oh no!" in result.exception
