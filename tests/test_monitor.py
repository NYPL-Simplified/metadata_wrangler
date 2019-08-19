from nose.tools import (
    eq_,
    set_trace,
)

from . import DatabaseTest

from core.model import Subject

from monitor import (
    FASTNameAssignmentMonitor,
)


class TestFASTNameAssignmentMonitor(DatabaseTest):

    def test_process_batch(self):

        # Create some Subjects -- some of them have names and some
        # don't.
        fast1 = self._subject(Subject.FAST, "fast1")
        fast2 = self._subject(Subject.FAST, "fast2")
        fast2.name = "Existing FAST name."

        lcsh1 = self._subject(Subject.LCSH, "lcsh1")
        lcsh2 = self._subject(Subject.LCSH, "lcsh2")
        lcsh2.name = "Existing LCSH name."

        # This Subject has a name but no identifier -- this probably
        # shouldn't happen.
        missing_identifier = self._subject(Subject.LCSH, "")
        missing_identifier.identifier = None
        missing_identifier.name = None

        tag = self._subject(Subject.TAG, "tag")

        # Mock FASTNames and LCSHNames objects -- a dict will do fine.
        fast = {
            "fast1": "FAST Name 1",
            "fast2": "FAST Name 2 (not used)",
        }
        lcsh = {
            "lcsh1" : "LCSH Name 1",
            "lcsh2" : "LCSH Name 2 (not used)",
        }

        monitor = FASTNameAssignmentMonitor(self._db, fast, lcsh)

        # item_query() finds only the FAST and LCSH subjects with an
        # identifier but no name.
        qu = monitor.item_query().order_by(Subject.id)
        eq_([fast1, lcsh1], qu.all())

        # Pass every Subject into process_item().
        for i in self._db.query(Subject):
            monitor.process_item(i)

        # The Subjects that already have names were left alone, even
        # though the name in the FAST data differs from what's in the
        # database.
        eq_("Existing LCSH name.", lcsh2.name)
        eq_("Existing FAST name.", fast2.name)

        # The Subject with no identifier was ignored.
        eq_(None, missing_identifier.name)

        # The 'tag:' Subject was ignored.
        eq_(None, tag.name)

        # The Subjects that would have shown up in item_query() have
        # been processed.
        eq_("FAST Name 1", fast1.name)
        eq_("LCSH Name 1", lcsh1.name)
