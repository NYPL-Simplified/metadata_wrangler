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
    get_one_or_create,
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

    def test_acquisition_feed_by_title(self):
        lane = "Foo"
        language="eng"

        w_z = self._work(title="Z", authors="AAA", languages=language,
                         lane=lane, with_license_pool=True)
        w_a = self._work(title="A", authors="ZZZ", languages=language,
                         lane=lane, with_license_pool=True)
        self._db.commit()

        # We get a feed...
        by_title = AcquisitionFeed.by_title(self._db, [language], lane)
        eq_("Foo: by title", by_title.title)
        assert by_title.url.endswith("/lanes/eng/Foo?order=title")

        # ...with two entries.
        eq_(2, len(by_title.entries))

        # The "A" title shows up first, even though it was created
        # second.
        eq_(["A", "Z"], [x.title for x in by_title.entries])

    def test_acquisition_feed_by_author(self):
        lane = "Foo"
        language="eng"

        w_z = self._work(title="A", authors="ZZZ", languages=language,
                         lane=lane, with_license_pool=True)
        w_a = self._work(title="Z", authors="AAA", languages=language,
                         lane=lane, with_license_pool=True)
        self._db.commit()

        # We get a feed...
        by_author = AcquisitionFeed.by_author(self._db, [language], lane)
        eq_("Foo: by author", by_author.title)
        assert by_author.url.endswith("/lanes/eng/Foo?order=author")

        # ...with two entries.
        eq_(2, len(by_author.entries))

        # The "AAA" title shows up first, even though it was created
        # second.
        names = []
        for entry in by_author.entries:
            names.append([author['name'] for author in entry.author])
        eq_([["AAA"], ["ZZZ"]], names)

    def test_acquisition_feed_gets_one_lane_only(self):
        work = self._work(lane="Foo", with_license_pool=True)
        work2 = self._work(lane="Bar", with_license_pool=True)
        by_title = AcquisitionFeed.by_title(self._db, "eng", "Foo")
        eq_(1, len(by_title.entries))
        eq_([work.title], [x.title for x in by_title.entries])

    def test_acquisition_feed_gets_only_specified_languages(self):
        lane="Foo"
        work = self._work(lane=lane, languages="eng", with_license_pool=True)
        work2 = self._work(lane=lane, languages="spa", with_license_pool=True)

        feed = AcquisitionFeed.by_title(self._db, "eng", lane)
        eq_(1, len(feed.entries))

        feed = AcquisitionFeed.by_title(self._db, ["eng", "spa"], lane)
        eq_(2, len(feed.entries))


    def test_acquisition_feed_omits_works_with_no_active_license_pool(self):
        lane = "Foo"
        work = self._work(lane=lane, with_license_pool=True)
        no_license_pool = self._work(lane=lane, with_license_pool=False)
        not_open_access = self._work(lane=lane, with_license_pool=True)
        not_open_access.license_pools[0].open_access = False
        self._db.commit()

        # We get a feed with only one entry--the one with an open-access
        # license pool.
        by_title = AcquisitionFeed.by_title(self._db, "eng", lane)
        eq_(1, len(by_title.entries))
        eq_([work.title], [x.title for x in by_title.entries])
