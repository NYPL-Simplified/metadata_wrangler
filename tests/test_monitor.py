from nose.tools import set_trace, eq_
from . import DatabaseTest
from ..monitor import IdentifierResolutionMonitor
from core.model import (
    Equivalency,
    DataSource,
)

class TestIdentifierResolutionMonitor(DatabaseTest):
    def setup(self):
        super(TestIdentifierResolutionMonitor, self).setup()
        self.monitor = IdentifierResolutionMonitor(self._db)

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
        exception = "Hello from the other siiiiide"
        unresolved, ignore = self._unresolved_identifier(self._identifier())
        processed_unresolved = self.monitor.process_failure(unresolved, exception)

        eq_(unresolved, processed_unresolved)
        eq_(True, unresolved.exception.endswith("siiiiide"))
