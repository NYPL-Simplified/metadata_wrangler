import os
from tempfile import mkdtemp
import random
from nose.tools import (
    eq_,
    assert_raises_regexp,
    set_trace,
)

from ..mirror import Mirror

class DummyResponse(object):
    def __init__(self, status_code, content):
        self.status_code = status_code
        self.content = content

class DummyHTTPMirror(Mirror):

    def __init__(self, *args):
        super(DummyHTTPMirror, self).__init__(*args)
        self.response_queue = []

    def queue_response(self, status_code, content):
        response = DummyResponse(status_code, content)
        self.response_queue.append(response)

    def make_request(self, *args):
        return self.response_queue.pop()

class TestMirror:

    def setUp(self):
        self.default_sleep_time = 22
        tmpdir = mkdtemp()
        self.dir = os.path.join(tmpdir, "mirror")
        self.mirror = DummyHTTPMirror(tmpdir, lambda x: self.default_sleep_time)

    def test_error_if_base_data_directory_does_not_exist(self):
        assert_raises_regexp(
            ValueError,
            "Base data directory .* does not exist",
            DummyHTTPMirror, "/tmp/no/such/directory/" + str(random.random()))

    def test_local_path(self):
        """Demonstrate the capabilities and the limitations of the mirror
        code."""

        eq_(os.path.join(self.dir, "foo.com/bar/baz"),
            self.mirror.local_path("http://foo.com/bar/baz#exc"))

        eq_(os.path.join(self.dir, "foo.com/bar/baz"),
            self.mirror.local_path("http://foo.com/bar/baz?extra_stuff"))

        eq_(None,
            self.mirror.local_path("http://foo.com/"))
        
        eq_(None,
            self.mirror.local_path("http://foo.com/suspicious/../../etc/passwd"))

        eq_(None,
            self.mirror.local_path("http://foo.com/../../etc/passwd"))

    def test_ensure_mirrored(self):

        # Let's mirror something
        self.mirror.queue_response(200, "foo")

        path, sleep_time = self.mirror.ensure_mirrored("http://foo.com/bar")
        eq_(self.default_sleep_time, sleep_time)
        eq_("foo", open(path).read())

        # Mirror it again, and it won't make another request. (If it
        # did make another request the test would crash because no
        # other response is queued up.)
        path, sleep_time = self.mirror.ensure_mirrored("http://foo.com/bar")
        eq_(0, sleep_time)

    def test_will_not_mirror_bad_urls(self):

        assert_raises_regexp(
            ValueError,
            "Cannot mirror URL due to its structure: .*",
            self.mirror.ensure_mirrored, "blahblah")

        assert_raises_regexp(
            ValueError,
            "Cannot mirror URL due to its structure: .*",
            self.mirror.ensure_mirrored, "http://foo.com/")
        
        assert_raises_regexp(
            ValueError,
            "Cannot mirror URL due to its structure: .*",
            self.mirror.ensure_mirrored, "http://foo.com/../etc/passwd")
