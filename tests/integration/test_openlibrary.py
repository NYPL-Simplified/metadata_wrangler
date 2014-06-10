
from nose.tools import set_trace, eq_
from tests.db import (
    setup_module,
    teardown_module,
    DatabaseTest,
)

from integration.openlibrary import OpenLibraryIDMapping

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
        eq_(['OL24385118M', 'OL24390638M'], sorted(
            mapping.oclc_to_ol['001299047']))

        eq_('6636377', mapping.ol_to_cover_id['OL24385118M'])
