
from nose.tools import set_trace, eq_

from tests.db import (
    DatabaseTest,
)

from integration.openlibrary import (
    OpenLibraryIDMapping,
    OpenLibraryMonitor,
)

from model import (
    DataSource,
    Resource,
    Identifier,
    Edition,
)

class TestOpenLibraryIDMapping(object):

    def non_oclc_data_ignored(self):
        """IDs that are not OCLC IDs are ignored."""
        data = [
            "oclc	001185458	/books/OL24334671M	6568609",
            "other junk	12312312	/books/213423423	5946t5",
        ]
        mapping = OpenLibraryIDMapping(data)
        eq_(1, len(mapping.ol_to_cover_id))
        eq_(1, len(mapping.oclc_to_ol))

    def test_mapping(self):
        data = [
            "oclc	001185458	/books/OL24334671M	6568609",
            "oclc	001274531	/books/OL24359594M	6587057",
            "oclc	001275038	/books/OL24358766M	6585908",
            "oclc	001287012	/books/OL24349933M	6534668",
            "oclc	001287012	/books/OL24391247M	6644880",
            "oclc	001299040	/books/OL24341407M	6523875",
            "oclc	001299047	/books/OL24385118M	6636377",
            "oclc	001299047	/books/OL24390638M	6643742",
        ]

        mapping = OpenLibraryIDMapping(data)
        # There are eight rows in the data. 
        eq_(8, len(mapping.ol_to_cover_id))

        # But only six distinct OCLC Numbers are represented.
        eq_(6, len(mapping.oclc_to_ol))

        eq_(['OL24359594M'], mapping.oclc_to_ol['001274531'])

        # This OCLC Number is mapped to two Open Library IDs.
        eq_(['OL24385118M', 'OL24390638M'], sorted(
            mapping.oclc_to_ol['001299047']))

        # Each Open Library ID is mapped to a unique cover ID.
        eq_('6636377', mapping.ol_to_cover_id['OL24385118M'])


class TestOpenLibraryMonitor(DatabaseTest):

    def test_monitor(self):
        data = [
            "oclc	001185458	/books/OL24334671M	6568609",
            "oclc	001274531	/books/OL24359594M	6587057",
            "oclc	001275038	/books/OL24358766M	6585908",
            "oclc	001287012	/books/OL24349933M	6534668",
            "oclc	001287012	/books/OL24391247M	6644880",
            "oclc	001299040	/books/OL24341407M	6523875",
            "oclc	111	/books/111M	-1",
            "oclc	001299047	/books/OL24385118M	6636377", # 1
            "oclc	001299047	/books/OL24390638M	6643742", # 2
        ]

        mapping = OpenLibraryIDMapping(data)

        # Two of these OCLC Numbers are in use.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "001299047")
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "111")
        self._db.commit()

        OpenLibraryMonitor().handle(self._db, mapping)

        # Those OCLC Numbers have been turned into two
        # Editions. No other Editions have been created.
        wr1, wr2 = self._db.query(Edition).all()

        # Each Edition corresponds to one of the "001299047" lines
        # in the original data mapping. The "111" line was not turned
        # into a Edition because OpenLibrary specified an invalid
        # cover ID (-1) for it.

        # Each existing Edition has been given a link to a
        # full image.
        id1 = wr1.primary_identifier
        eq_("OL24385118M", id1.identifier)
        [link1] = id1.resources
        eq_(DataSource.OPEN_LIBRARY, link1.data_source.name)
        eq_(Resource.IMAGE, link1.rel)
        eq_('http://covers.openlibrary.org/b/id/6636377-L.jpg',
            link1.href)

        id2 = wr2.primary_identifier
        eq_("OL24390638M", id2.identifier)
        [link2] = id2.resources
        eq_(DataSource.OPEN_LIBRARY, link2.data_source.name)
        eq_(Resource.IMAGE, link2.rel)
        eq_('http://covers.openlibrary.org/b/id/6643742-L.jpg',
            link2.href)

