from nose.tools import (
    eq_,
    set_trace,
)

from tests.db import (
    setup_module, 
    teardown_module, 
    DatabaseTest,
)

from model import (
    Work,
)

from lane import Lane

from opds import (
    AcquisitionFeed,
    NavigationFeed,
)

class TopLevel(Lane):
    name = "Toplevel"
    sublanes = set([])

class ParentLane(Lane):
    name = "Parent"
    sublanes = set([])    
TopLevel.sublanes.add(ParentLane)

class ChildLane(Lane):
    name = "Child"
    sublanes = set([])
ParentLane.sublanes.add(ChildLane)

class TestOPDS(DatabaseTest):
    
    def test_navigation_feed(self):
        feed = NavigationFeed.main_feed(TopLevel, ["eng", "spa"])
        assert feed.url.endswith("/lanes/eng%2Cspa")

        # Every lane has an entry.
        eq_(3, len(feed.entries))
        tags = [x.title for x in feed.entries]
        eq_(['Child', 'Parent', 'Toplevel'], sorted(tags))

        # Let's take one entry as an example.
        toplevel = [x for x in feed.entries if x.title == 'Toplevel'][0]
        eq_("tag:eng,spa:Toplevel", toplevel.id)

        # There are three links to acquisition feeds.
        recommended, author, title = sorted(toplevel.links)

        assert recommended['href'].endswith("/lanes/eng%2Cspa/Toplevel")
        eq_("Recommended", recommended['title'])
        eq_(NavigationFeed.RECOMMENDED_REL, recommended['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, recommended['type'])

        assert author['href'].endswith("/lanes/eng%2Cspa/Toplevel?order=author")
        eq_("By author", author['title'])
        eq_("subsection", author['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, author['type'])

        assert title['href'].endswith("/lanes/eng%2Cspa/Toplevel?order=title")
        eq_("By title", title['title'])
        eq_("subsection", title['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, title['type'])

