from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from core import mirror
from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.mirror import MirrorUploader
from core.model import (
    DataSource,
    ExternalIntegration,
)
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


class TestContentCafeAPI(DatabaseTest):

    def test_from_config(self):
        # Without an integration, an error is raised.
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        # With incomplete integrations, an error is raised.
        integration = self._external_integration(
            ExternalIntegration.CONTENT_CAFE,
            goal=ExternalIntegration.METADATA_GOAL,
            username=u'yup'
        )
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = None
        integration.password = u'yurp'
        assert_raises(
            CannotLoadConfiguration, ContentCafeAPI.from_config, self._db
        )

        integration.username = u'yup'
        result = ContentCafeAPI.from_config(
            self._db, soap_client=object()
        )
        eq_(True, isinstance(result, ContentCafeAPI))

        # NOTE: We can't test the case where soap_client is not
        # mocked, because the ContentCafeSOAPClient constructor makes
        # a real HTTP request to load its WSDL file. We might be able
        # to improve this by seeing how mockable SudsClient is, or by
        # mocking ContentCafeAPISOAPClient.WSDL_URL as a file:// URL.


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
                return mock_mirror
        # The content_cafe module has already imported MirrorUploader
        # from core/mirror, so we need to mock it there rather than
        # mocking mirror.
        content_cafe.MirrorUploader = MockUploader

        # Now we can invoke the constructor with no special arguments
        # and our mocked defaults will be used.
        provider = ContentCafeCoverageProvider(self._default_collection)
        eq_(mock_mirror, provider.replacement_policy.mirror)
        eq_(mock_api, provider.content_cafe)

        # Restore mocked classes
        content_cafe.ContentCafeAPI = ContentCafeAPI
        content_cafe.MirroUploader = MirrorUploader

    def test_process_item_success(self):
        class MockMetadata(object):
            def __init__(self, identifier):
                self.identifier = identifier

            def apply(self, *args, **kwargs):
                self.apply_called_with = (args, kwargs)

        class MockContentCafeAPI(object):
            """Pretend that we went to Content Cafe and got some Metadata."""
            def create_metadata(self, identifier):
                self.metadata = MockMetadata(identifier)
                return self.metadata
        api = MockContentCafeAPI()

        provider = ContentCafeCoverageProvider(
            self._default_collection, api, object()
        )
        identifier = self._identifier()

        # process_item indicates success by returning the Identifier
        # it was given.
        eq_(identifier, provider.process_item(identifier))

        # An Edition has been created representing Content Cafe's
        # take on this book.
        [edition] = identifier.primarily_identifies
        eq_(DataSource.CONTENT_CAFE, edition.data_source.name)

        # MockContentCafeAPI.create_metadata(identifier) was called.
        metadata = api.metadata
        eq_(identifier, metadata.identifier)

        # And then apply() was called on the resulting MockMetadata
        # object.
        args, kwargs = metadata.apply_called_with
        eq_((edition,), args)
        eq_(None, kwargs['collection'])
        eq_(provider.replacement_policy, kwargs['replace'])

    def test_process_item_failure_not_found(self):
        """Test what happens when Content Cafe hasn't heard of
        an Identifier.
        """

        class NotFoundContentAPI(object):
            def create_metadata(self, *args, **kwargs):
                return None

        provider = ContentCafeCoverageProvider(
            self._default_collection, api=NotFoundContentAPI(),
            mirror=object()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        eq_("Content Cafe has no knowledge of this identifier.",
            result.exception)

    def test_process_item_exception(self):
        """Test what happens when an exception is raised
        in the course of obtaining coverage.
        """
        class CantCreateMetadata(object):
            def create_metadata(self, *args, **kwargs):
                raise Exception("Oh no!")

        provider = ContentCafeCoverageProvider(
            self._default_collection, api=CantCreateMetadata(),
            mirror=object()
        )
        identifier = self._identifier()
        result = provider.process_item(identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(identifier, result.obj)
        assert "Oh no!" in result.exception
