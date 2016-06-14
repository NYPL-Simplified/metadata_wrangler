from nose.tools import (
    eq_,
    set_trace,
)
import os

from . import DatabaseTest
from core.model import (
    DataSource,
    Identifier,
)
from core.opds_import import MockSimplifiedOPDSLookup
from content_server import ContentServerCoverageProvider

class TestContentServerCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestContentServerCoverageProvider, self).setup()
        self.lookup = MockSimplifiedOPDSLookup("http://url/")
        self.provider = ContentServerCoverageProvider(
            self._db, content_server=self.lookup
        )
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "opds")

    def sample_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        return open(path).read()

    def test_success(self):
        data = self.sample_data("content_server_lookup.opds")
        self.lookup.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Make the Identifier match the book the queued-up response is
        # talking about
        identifier.identifier = "20201"
        success = self.provider.process_item(identifier)
        eq_(success, identifier)

        # The book was imported and turned into a Work.
        work = identifier.licensed_through.work
        eq_("Mary Gray", work.title)

        # It's not presentation-ready yet, because we are the metadata
        # wrangler and our work is not yet done.
        eq_(False, work.presentation_ready)

    def test_no_such_work(self):
        data = self.sample_data("no_such_work.opds")
        self.lookup.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Make the Identifier match the book the queued-up response is
        # talking about
        identifier.identifier = "2020110"
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("404: I've never heard of this work.", failure.exception)
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)

        # Most of the time this is a persistent error but it's
        # possible that we know about a book the content server
        # doesn't know about yet.
        eq_(True, failure.transient)

    def test_wrong_work_in_response(self):
        data = self.sample_data("content_server_lookup.opds")
        self.lookup.queue_response(
            200, {"content-type": "application/atom+xml"},
            content=data
        )
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        # The content server told us about a different book than the
        # one we asked about.
        identifier.identifier = "999"
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_('Identifier was not mentioned in lookup response', failure.exception)
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)
        eq_(True, failure.transient)

    def test_content_server_http_failure(self):
        """Test that HTTP-level failures of the content server
        become transient CoverageFailures.
        """
        identifier = self._identifier(identifier_type=Identifier.GUTENBERG_ID)

        self.lookup.queue_response(
            500, content="help me!"
        )
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("Got status code 500 from external server, cannot continue.",
            failure.exception)
        eq_(True, failure.transient)

        self.lookup.queue_response(
            200, {"content-type": "text/plain"}, content="help me!"
        )
        failure = self.provider.process_item(identifier)
        eq_(identifier, failure.obj)
        eq_("Content Server served unhandleable media type: text/plain",
            failure.exception)
        eq_(True, failure.transient)
