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
        record2, was_new = WorkRecord.for_foreign_id(
            self._db, data_source_2, WorkIdentifier.OCLC_NUMBER, "22")

        eq = record.primary_identifier.equivalent_to(
            self._db, data_source_2, record2.primary_identifier)

        eq_(eq.input, record.primary_identifier)
        eq_(eq.output, record2.primary_identifier)
        eq_(eq.data_source, data_source_2)

        eq_([eq], record.primary_identifier.equivalencies)

        eq_([record2], record.equivalent_work_records(self._db).all())

    def test_recursively_equivalent_identifiers(self):

        # We start with a Gutenberg book.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        record, ignore = WorkRecord.for_foreign_id(
            self._db, gutenberg, WorkIdentifier.GUTENBERG_ID, "100")
        gutenberg_id = record.primary_identifier

        # We use OCLC Classify to do a title/author lookup.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        search_id, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.OCLC_TITLE_AUTHOR_SEARCH,
            "Moby Dick/Herman Melville")
        gutenberg_id.equivalent_to(self._db, oclc, search_id)

        # The title/author lookup associates the search term with two
        # different OCLC Numbers.
        oclc_id, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.OCLC_NUMBER, "9999")
        oclc_id_2, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.OCLC_NUMBER, "1000")

        search_id.equivalent_to(self._db, oclc, oclc_id)
        search_id.equivalent_to(self._db, oclc, oclc_id_2)

        # We then use OCLC Linked Data to connect one of the OCLC
        # Numbers with an ISBN.
        linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        isbn_id, ignore = WorkIdentifier.for_foreign_id(
            self._db, WorkIdentifier.ISBN, "900100434X")
        oclc_id.equivalent_to(self._db, linked_data, isbn_id)

        levels = [
            record.recursively_equivalent_identifiers(self._db, i) 
            for i in range(0,4)]

        # At level 0, the only identifier found is the Gutenberg ID.
        eq_(set([gutenberg_id]), set(levels[0]))

        # At level 1, we pick up the title/author lookup.
        eq_(set([gutenberg_id, search_id]), set(levels[1]))

        # At level 2, we pick up the title/author lookup and the two
        # OCLC Numbers.
        eq_(set([gutenberg_id, search_id, oclc_id, oclc_id_2]), set(levels[2]))

        # At level 3, we also pick up the ISBN.
        eq_(set([gutenberg_id, search_id, oclc_id, oclc_id_2, isbn_id]), set(levels[3]))
