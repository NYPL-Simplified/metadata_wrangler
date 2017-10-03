import os
from nose.tools import (
    eq_,
    set_trace,
)
from core.s3 import DummyS3Uploader
from core.testing import (
    DatabaseTest,
    DummyHTTPClient,
)
from core.model import Identifier
from core.overdrive import MockOverdriveAPI
from overdrive import OverdriveBibliographicCoverageProvider

class TestOverdriveBibliographicCoverageProvider(DatabaseTest):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files")

    def data_file(self, path):
        """Return the contents of a test data file."""
        return open(os.path.join(self.resource_path, path)).read()

    def test_replacement_policy_uses_provided_mirror(self):
        collection = MockOverdriveAPI.mock_collection(self._db)
        mirror = DummyS3Uploader()
        provider = OverdriveBibliographicCoverageProvider(
                collection, uploader=mirror, api_class=MockOverdriveAPI
        )
        
        # Any resources discovered by Overdrive will be
        # sent through this mirror.
        eq_(mirror, provider.replacement_policy.mirror)

        http = DummyHTTPClient()
        provider.replacement_policy.http_get = http.do_get

        # Now let's try looking up a specific identifier through 'Overdrive'.
        identifier = self._identifier(
            Identifier.OVERDRIVE_ID, "3896665d-9d81-4cac-bd43-ffc5066de1f5"
        )


        body = self.data_file("overdrive/overdrive_metadata.json")
        provider.api.queue_response(200, {}, body)

        test_cover = self.data_file("covers/test-book-cover.png")
        test_small_cover = self.data_file("covers/tiny-image-cover.png")

        # Overdrive's full-sized image -- we will be creating our own
        # thumbnail from this.
        http.queue_response(200, "image/jpeg", {}, test_cover)

        # Overdrive's thumbnail image -- we will not be using this
        http.queue_response(200, "image/jpeg", {}, test_small_cover)

        record = provider.ensure_coverage(identifier)
        eq_("success", record.status)

        # The full image and the thumbnail have been uploaded to
        # the fake S3.
        full, thumbnail = mirror.uploaded
        eq_(test_cover, full.content)

        # The URLs for the Resource objects are our S3 URLs, not Overdrive's
        # URLs.
        expect = "Overdrive/Overdrive%%20ID/%s" % identifier.identifier
        for url in [full.mirror_url, thumbnail.mirror_url]:
            assert expect in url
        assert "/scaled/" in thumbnail.mirror_url
        assert "/scaled/" not in full.mirror_url

        # The thumbnail is a newly created image that is not the
        # same as the full image or the test cover.
        assert thumbnail.content != test_small_cover
        assert thumbnail.content != test_cover
