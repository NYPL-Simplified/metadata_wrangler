from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from core import mirror
from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.mirror import MirrorUploader
from core.model import ExternalIntegration
from core.s3 import (
    MockS3Uploader,
    S3Uploader,
)

from . import (
    DatabaseTest,
)
from content_cafe import (
    ContentCafeAPI,
    ContentCafeCoverageProvider,
)
import content_cafe

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
        mock_api = object()
        mock_mirror = object()
        provider = ContentCafeCoverageProvider(
            self._default_collection, api=mock_api, mirror=mock_mirror
        )
        eq_(self._default_collection, provider.collection)
        eq_(mock_mirror, provider.replacement_policy.mirror)
        eq_(mock_api, provider.content_cafe)

        # If no ContentCafeAPI is provided, the output of
        # ContentCafeAPI.from_config is used.
        #
        # If no MirrorUploader is provided, the output of
        # S3Uploader.sitewide is used.
        class MockContentCafeAPI(ContentCafeAPI):
            @classmethod
            def from_config(cls, *args, **kwargs):
                return mock_api
        content_cafe.ContentCafeAPI = MockContentCafeAPI

        class MockUploader(MirrorUploader):
            @classmethod
            def sitewide(cls, *args, **kwargs):
                return mock_uploader
        # The content_cafe module has already imported MirrorUploader
        # from core/mirror, so we need to mock it there rather than
        # mocking mirror.
        content_cafe.MirrorUploader = MockUploader

        provider = ContentCafeCoverageProvider(self._default_collection)
        eq_(mock_mirror, provider.replacement_policy.mirror)
        eq_(mock_api, provider.content_cafe)

        # Restore mocked classes
        content_cafe.ContentCafeAPI = ContentCafeAPI
        content_cafe.MirroUploader = MirrorUploader

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
