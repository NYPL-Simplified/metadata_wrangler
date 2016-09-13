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
    LicensePool,
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

from core.s3 import DummyS3Uploader


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
        uploader = DummyS3Uploader()
        self.coverage_provider = IdentifierResolutionCoverageProvider(
            self._db, uploader=uploader, providers=([], [])
        )

        self.always_successful = AlwaysSuccessfulCoverageProvider(
            "Always", [self.identifier.type], self.source
        )
        self.never_successful = NeverSuccessfulCoverageProvider(
            "Never", [self.identifier.type], self.source
        )
        self.broken = BrokenCoverageProvider(
            "Broken", [self.identifier.type], self.source
        )

    def test_process_item_creates_license_pool(self):
        self.coverage_provider.required_coverage_providers = [
            self.always_successful
        ]

        self.coverage_provider.process_item(self.identifier)
        lp = self.identifier.licensed_through
        eq_(True, isinstance(lp, LicensePool))
        eq_(lp.data_source, self.coverage_provider.output_source)

    def test_process_item_succeeds_if_all_required_coverage_providers_succeed(self):
        self.coverage_provider.required_coverage_providers = [
            self.always_successful, self.always_successful
        ]

        # The coverage provider succeeded and returned an identifier.
        result = self.coverage_provider.process_item(self.identifier)
        eq_(result, self.identifier)

    def test_process_item_fails_if_any_required_coverage_providers_fail(self):
        self.coverage_provider.required_coverage_providers = [
            self.always_successful, self.never_successful
        ]

        result = self.coverage_provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_("500: What did you expect?", result.exception)
        eq_(False, result.transient)

        # The failure type of the IdentifierResolutionCoverageProvider
        # coverage record matches the failure type of the required provider's
        # coverage record.
        self.never_successful.transient = True
        result = self.coverage_provider.process_item(self.identifier)
        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.transient)

    def test_process_item_fails_when_required_provider_raises_exception(self):
        self.coverage_provider.required_coverage_providers = [self.broken]
        result = self.coverage_provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        eq_(True, result.transient)

    def test_process_item_fails_when_finalize_raises_exception(self):
        class FinalizeAlwaysFails(IdentifierResolutionCoverageProvider):
            def finalize(self, unresolved_identifier):
                raise Exception("Oh no!")

        provider = FinalizeAlwaysFails(
            self._db, uploader=DummyS3Uploader(), providers=([], [])
        )
        result = provider.process_item(self.identifier)

        eq_(True, isinstance(result, CoverageFailure))
        assert "Oh no!" in result.exception
        eq_(True, result.transient)

    def test_process_item_succeeds_when_optional_provider_fails(self):
        self.coverage_provider.required_coverage_providers = [
            self.always_successful, self.always_successful
        ]

        self.coverage_provider.optional_coverage_providers = [
            self.always_successful, self.never_successful
        ]

        result = self.coverage_provider.process_item(self.identifier)

        # A successful result is achieved, even though the optional
        # coverage provider failed.
        eq_(result, self.identifier)

        # An appropriate coverage record was created to mark the failure.
        presentation_edition = DataSource.lookup(
            self._db, DataSource.PRESENTATION_EDITION
        )
        r = self._db.query(CoverageRecord).filter(
            CoverageRecord.identifier==self.identifier,
            CoverageRecord.data_source!=presentation_edition).one()
        eq_("What did you expect?", r.exception)
