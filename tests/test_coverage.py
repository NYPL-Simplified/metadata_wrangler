from nose.tools import (
    eq_,
    set_trace,
)
import os

from . import DatabaseTest

from core.model import (
    CoverageRecord, 
    DataSource,
    get_one, 
    Identifier,
)
from core.coverage import CoverageFailure
from core.opds_import import MockSimplifiedOPDSLookup

from content_server import ContentServerCoverageProvider

from coverage import IdentifierResolutionCoverageProvider

from core.testing import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
    BrokenCoverageProvider,
)



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
        eq_(DataSource.OA_CONTENT_SERVER, failure.data_source.name)


class TestIdentifierResolutionCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestIdentifierResolutionCoverageProvider, self).setup()
        self.identifier = self._identifier(Identifier.OVERDRIVE_ID)
        self.source = DataSource.license_source_for(self._db, self.identifier)
        self.coverage_provider = IdentifierResolutionCoverageProvider(self._db)

        self.always_successful = AlwaysSuccessfulCoverageProvider(
            "Always", [self.identifier.type], self.source
        )
        self.never_successful = NeverSuccessfulCoverageProvider(
            "Never", [self.identifier.type], self.source
        )
        self.broken = BrokenCoverageProvider("Broken", [self.identifier.type], self.source)

    def test_process_item_is_successful_if_required_coverage_providers_are_successful(self):
        self.coverage_provider.required_coverage_providers = [self.always_successful]

        # The coverage provider succeeded and returned an identifier.
        result = self.coverage_provider.process_item(self.identifier)
        eq_(result, self.identifier)

    def test_process_item_fails_if_any_required_coverage_providers_fail(self):
        self.coverage_provider.required_coverage_providers = [
            self.always_successful, self.never_successful
        ]

        result = self.coverage_provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_("500; What did you expect?", result.exception)

        # The failure type of the IdentifierResolutionCoverageProvider coverage
        # record matches the failure type of the required provider's coverage
        # record.
        pass

    def test_run_once_fails_when_required_provider_raises_exception(self):
        m = IdentifierResolutionMonitor(
            self._db, "Will raise exception",
            required_coverage_providers=[self.broken]
        )

        m.run_once()

        # The exception was recorded in the UnresolvedIdentifier object.
        assert "I'm too broken to even return a CoverageFailure." in self.ui.exception


    def test_run_once_fails_when_finalize_raises_exception(self):
        class FinalizeAlwaysFails(IdentifierResolutionMonitor):
            def finalize(self, unresolved_identifier):
                raise Exception("Oh no!")

        m = FinalizeAlwaysFails(self._db, "Always fails")
        ui, ignore = self._unresolved_identifier()
        m.run_once(ui)
        eq_(500, ui.status)
        assert "Oh no!" in ui.exception

    def test_resolve_succeeds_when_optional_provider_fails(self):
        ui, ignore = self._unresolved_identifier()
        identifier = self.ui.identifier
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        p = NeverSuccessfulCoverageProvider(
            "Never", [identifier.type], source
        )
        m = IdentifierResolutionMonitor(
            self._db, "Will fail but it's OK",
            optional_coverage_providers=[p]
        )

        success = m.resolve(self.ui)

        # The coverage provider failed and an appropriate coverage record
        # was created to mark the failure.
        r = get_one(self._db, CoverageRecord, identifier=self.identifier)
        eq_("What did you expect?", r.exception)

        # But because it was an optional CoverageProvider that failed,
        # no exception was raised and resolve() returned True to indicate
        # success.
        eq_(success, True)
