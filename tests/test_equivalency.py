from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from model import (
    CirculationEvent,
    DataSource,
    get_one_or_create,
    Work,
    LicensePool,
    WorkIdentifier,
    WorkRecord,
)

from tests.db import (
    setup_module, 
    teardown_module, 
    DatabaseTest,
)

class TestEquivalency(DatabaseTest):

    def test_register_equivalency(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"

        # We've got a record.
        record, was_new = WorkRecord.for_foreign_id(
            self._db, data_source, WorkIdentifier.GUTENBERG_ID, id)

        # Then we look it up and discover another identifier for it.
        data_source_2 = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_number, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.OCLC_NUMBER, "22")

        eq = record.primary_identifier.equivalent_to(
            self._db, data_source_2, oclc_number)

        eq_(eq.input, record.primary_identifier)
        eq_(eq.output, oclc_number)
        eq_(eq.data_source, data_source_2)

        eq_([eq], list(record.primary_identifier.equivalencies))
