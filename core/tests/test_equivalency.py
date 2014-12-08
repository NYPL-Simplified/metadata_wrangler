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
    Identifier,
    Edition,
)

from testing import (
    DatabaseTest,
)

class TestEquivalency(DatabaseTest):

    def test_register_equivalency(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"

        # We've got a record.
        record, was_new = Edition.for_foreign_id(
            self._db, data_source, Identifier.GUTENBERG_ID, id)

        # Then we look it up and discover another identifier for it.
        data_source_2 = DataSource.lookup(self._db, DataSource.OCLC)
        record2, was_new = Edition.for_foreign_id(
            self._db, data_source_2, Identifier.OCLC_NUMBER, "22")

        eq = record.primary_identifier.equivalent_to(
            data_source_2, record2.primary_identifier, 1)

        eq_(eq.input, record.primary_identifier)
        eq_(eq.output, record2.primary_identifier)
        eq_(eq.data_source, data_source_2)

        eq_([eq], record.primary_identifier.equivalencies)

        eq_([record, record2], record.equivalent_editions().all())

    def test_recursively_equivalent_identifiers(self):

        # We start with a Gutenberg book.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        record, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "100")
        gutenberg_id = record.primary_identifier

        # We use OCLC Classify to do a title/author lookup.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        search_id, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_WORK,
            "60010")
        gutenberg_id.equivalent_to(oclc, search_id, 1)

        # The title/author lookup associates the search term with two
        # different OCLC Numbers.
        oclc_id, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "9999")
        oclc_id_2, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "1000")

        search_id.equivalent_to(oclc, oclc_id, 1)
        search_id.equivalent_to(oclc, oclc_id_2, 1)

        # We then use OCLC Linked Data to connect one of the OCLC
        # Numbers with an ISBN.
        linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        isbn_id, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "900100434X")
        oclc_id.equivalent_to(linked_data, isbn_id, 1)

        # As it turns out, we have an Overdrive work record...
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        overdrive_record, ignore = Edition.for_foreign_id(
            self._db, overdrive, Identifier.OVERDRIVE_ID, "{111-222}")
        overdrive_id = overdrive_record.primary_identifier

        # ...which is tied (by Overdrive) to the same ISBN.
        overdrive_id.equivalent_to(overdrive, isbn_id, 1)

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        gutenberg2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "200")
        gutenberg2.title = "Unrelated Gutenberg record."

        levels = [
            record.equivalent_identifier_ids(i) 
            for i in range(0,5)]

        # At level 0, the only identifier found is the Gutenberg ID.
        eq_(set([gutenberg_id.id]), set(levels[0]))

        # At level 1, we pick up the title/author lookup.
        eq_(set([gutenberg_id.id, search_id.id]), set(levels[1]))

        # At level 2, we pick up the title/author lookup and the two
        # OCLC Numbers.
        eq_(set([gutenberg_id.id, search_id.id, oclc_id.id, oclc_id_2.id]), set(levels[2]))

        # At level 3, we also pick up the ISBN.
        eq_(set([gutenberg_id.id, search_id.id, oclc_id.id, oclc_id_2.id, isbn_id.id]), set(levels[3]))

        # At level 4, the recursion starts to go in the other
        # direction: we pick up the Overdrive ID that's equivalent to
        # the same ISBN as the OCLC Number.
        eq_(set([gutenberg_id.id, search_id.id, oclc_id.id, oclc_id_2.id, isbn_id.id, overdrive_id.id]), set(levels[4]))
