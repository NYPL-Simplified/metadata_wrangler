from nose.tools import set_trace, eq_
from . import DatabaseTest
from ..monitor import IdentifierResolutionMonitor
from ..core.model import (
    DataSource,
    Identifier,
)

class DummyIdentifierResolutionMonitor(IdentifierResolutionMonitor):
    """An IdentifierRestolutionMonitor for testing"""

    def __init__(self):
        self.service_name = "Dummy Identifier Resolution Monitor"

    @property
    def providers(self):
        provider_types = [
            Identifier.ISBN, Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
            [Identifier.THREEM_ID, Identifier.ISBN],
            [Identifier.GUTENBERG_ID, Identifier.OCLC_WORK]
        ]
        return [DummyCoverageProvider(types) for types in provider_types]


class DummyCoverageProvider(object):
    def __init__(self, identifier_types):
        if not isinstance(identifier_types, list):
            identifier_types = [identifier_types]
        self.input_identifier_types = identifier_types


class TestIdentifierResolutionMonitor(DatabaseTest):
    def setup(self):
        super(TestIdentifierResolutionMonitor, self).setup()
        self.monitor = DummyIdentifierResolutionMonitor()

    def test_eligible_providers_for(self):
        gutenberg = self._identifier()
        threem = self._identifier(identifier_type=Identifier.THREEM_ID)
        oclc = self._identifier(identifier_type=Identifier.OCLC_WORK)
        isbn = self._identifier(identifier_type=Identifier.ISBN)

        gutenberg_providers = self.monitor.eligible_providers_for(gutenberg)
        threem_providers = self.monitor.eligible_providers_for(threem)
        oclc_providers = self.monitor.eligible_providers_for(oclc)
        isbn_providers = self.monitor.eligible_providers_for(isbn)

        eq_(1, len(gutenberg_providers))
        eq_(2, len(threem_providers))
        eq_(1, len(oclc_providers))
        eq_(2, len(isbn_providers))

    def test_has_unresolved_equivalents(self):
        identifier = self._identifier()
        eq_(False, self.monitor.has_unresolved_equivalents(identifier))

        eq_identifier = self._identifier()
        unresolved = self._unresolved_identifier(eq_identifier)
        identifier.equivalent_to(
            DataSource.lookup(self._db, DataSource.GUTENBERG), eq_identifier, 1
        )
        self._db.commit()
        eq_(True, self.monitor.has_unresolved_equivalents(identifier))

    def test_process_failure(self):
        exception = u"Hello from the other siiiiide"
        unresolved, ignore = self._unresolved_identifier(self._identifier())
        processed_unresolved = self.monitor.process_failure(unresolved, exception)

        eq_(unresolved, processed_unresolved)
        eq_(True, unresolved.exception.endswith("siiiiide"))
