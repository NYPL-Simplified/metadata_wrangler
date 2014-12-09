import feedparser
import datetime
from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    get_one_or_create,
    DataSource,
    Genre,
    LaneList,
    Lane,
    Patron,
    Work,
)

from ..core.opds import (
    OPDSFeed,
    AcquisitionFeed,
    NavigationFeed,
    URLRewriter
)

from ..core.classifier import (
    Classifier,
    Fantasy,
)

from circulation import app

class TestURLRewriter(object):

    def test_gutenberg_rewrite(self):

        u = URLRewriter.rewrite(
            "http://www.gutenberg.org/ebooks/126.epub.noimages")
        assert u.endswith("/126/pg126.epub")

        u = URLRewriter.rewrite(
            "http://www.gutenberg.org/ebooks/32975.epub.images")
        assert u.endswith("/32975/pg32975-images.epub")

        u = URLRewriter.rewrite(
            "http://www.gutenberg.org/cache/epub/24270/pg24270.cover.medium.jpg")
        assert u.endswith("/24270/pg24270.cover.medium.jpg")

class TestOPDS(DatabaseTest):

    def setup(self):
        super(TestOPDS, self).setup()
        self.app = app.test_client()
        self.ctx = app.test_request_context()
        self.ctx.push()

        self.lanes = LaneList.from_description(
            self._db,
            None,
            [dict(name="Fiction",
                  fiction=True,
                  audience=Classifier.AUDIENCE_ADULT,
                  genres=[]),
             Fantasy,
             dict(
                 name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audience=Classifier.AUDIENCE_YOUNG_ADULT,
                 genres=[]),
             dict(name="Romance", fiction=True, genres=[],
                  sublanes=[
                      dict(name="Contemporary Romance")
                  ]
              ),
         ]
        )

        class FakeConf(object):
            name = None
            sublanes = None
            pass

        self.conf = FakeConf()
        self.conf.sublanes = self.lanes
    
    def test_navigation_feed(self):
        original_feed = NavigationFeed.main_feed(self.conf)
        parsed = feedparser.parse(unicode(original_feed))
        feed = parsed['feed']

        # There's a self link.
        alternate, self_link, start_link = sorted(feed.links)
        assert self_link['href'].endswith("/lanes/")

        # There's a link to the top level, which is the same as the
        # self link.
        assert start_link['href'].endswith("/lanes/")
        eq_("start", start_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, start_link['type'])

        # Every lane has an entry.
        eq_(4, len(parsed['entries']))
        tags = [x['title'] for x in parsed['entries']]
        eq_(['Fantasy', 'Fiction', 'Romance', 'Young Adult'], sorted(tags))

        # Let's look at one entry, Fiction, which has no sublanes.
        toplevel = [x for x in parsed['entries'] if x.title == 'Fiction'][0]
        eq_("tag:Fiction", toplevel.id)

        # There are two links to acquisition feeds.
        self_link, featured, by_author = sorted(toplevel['links'])
        assert featured['href'].endswith("/feed/Fiction")
        eq_("Featured", featured['title'])
        eq_(NavigationFeed.FEATURED_REL, featured['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, featured['type'])

        assert by_author['href'].endswith("/feed/Fiction?order=author")
        eq_("Look inside Fiction", by_author['title'])
        # eq_(None, by_author.get('rel'))
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, by_author['type'])

        # Now let's look at one entry, Romance, which has a sublane.
        toplevel = [x for x in parsed['entries'] if x.title == 'Romance'][0]
        eq_("tag:Romance", toplevel.id)

        # Instead of an acquisition feed (by author), we have a navigation feed
        # (the sublanes of Romance).
        self_link, featured, sublanes = sorted(toplevel['links'])
        assert sublanes['href'].endswith("/lanes/Romance")
        eq_("Look inside Romance", sublanes['title'])
        eq_("subsection", sublanes['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, sublanes['type'])

    def test_navigation_feed_for_sublane(self):
        original_feed = NavigationFeed.main_feed(self.conf.sublanes.by_name['Romance'])
        parsed = feedparser.parse(unicode(original_feed))
        feed = parsed['feed']

        start_link, up_link, alternate_link, self_link = sorted(feed.links)

        # There's a self link.
        assert self_link['href'].endswith("/lanes/Romance")
        eq_("self", self_link['rel'])

        # There's a link to the top level.
        assert start_link['href'].endswith("/lanes/")
        eq_("start", start_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, start_link['type'])

        # There's a link to one level up.
        assert up_link['href'].endswith("/lanes/")
        eq_("up", up_link['rel'])
        eq_(NavigationFeed.NAVIGATION_FEED_TYPE, up_link['type'])


    def test_acquisition_feed(self):
        work = self._work(with_open_access_download=True, authors="Alice")

        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        parsed = feedparser.parse(u)
        [with_author] = parsed['entries']
        eq_("Alice", with_author['authors'][0]['name'])


    def test_acquisition_feed_includes_open_access_or_borrow_link(self):
        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        w2.license_pools[0].open_access = False
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        open_access_links, borrow_links = [x['links'] for x in entries]
        open_access_rels = [x['rel'] for x in open_access_links]
        assert OPDSFeed.OPEN_ACCESS_REL in open_access_rels
        assert not OPDSFeed.BORROW_REL in open_access_rels

        borrow_rels = [x['rel'] for x in borrow_links]
        assert not OPDSFeed.OPEN_ACCESS_REL in borrow_rels
        assert OPDSFeed.BORROW_REL in borrow_rels

    def test_acquisition_feed_includes_author_tag_even_when_no_author(self):
        work = self._work(with_open_access_download=True)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        assert "<author>" in u

    def test_acquisition_feed_contains_facet_links(self):
        work = self._work(with_open_access_download=True)

        def facet_url_generator(facet):
            return "http://blah/" + facet

        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               works, facet_url_generator, "author")
        u = unicode(feed)
        parsed = feedparser.parse(u)
        by_title = parsed['feed']

        alternate_link, by_author, by_title, self_link = sorted(
            by_title['links'], key=lambda x: (x['rel'], x.get('title')))

        eq_("http://the-url.com/", self_link['href'])

        # As we'll see below, the feed parser parses facetGroup as
        # facetgroup and activeFacet as activefacet. As we see here,
        # that's not a problem with the generator code.
        assert 'opds:facetgroup' not in u
        assert 'opds:facetGroup' in u
        assert 'opds:activefacet' not in u
        assert 'opds:activeFacet' in u

        eq_('Sort by', by_author['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_author['rel'])
        eq_('true', by_author['opds:activefacet'])
        eq_('Author', by_author['title'])
        eq_(facet_url_generator("author"), by_author['href'])

        eq_('Sort by', by_title['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_title['rel'])
        eq_('Title', by_title['title'])
        assert not 'opds:activefacet' in by_title
        eq_(facet_url_generator("title"), by_title['href'])

    def test_acquisition_feed_includes_available_and_issued_tag(self):
        today = datetime.date.today()
        today_s = today.strftime("%Y-%m-%d")
        the_past = today - datetime.timedelta(days=2)
        the_past_s = the_past.strftime("%Y-%m-%d")

        # This work has both issued and published. issued will be used
        # for the dc:dateCopyrighted tag.
        work1 = self._work(with_open_access_download=True)
        work1.primary_edition.issued = today
        work1.primary_edition.published = the_past
        work1.license_pools[0].availability_time = the_past

        # This work only has published. published will be used for the
        # dc:dateCopyrighted tag.
        work2 = self._work(with_open_access_download=True)
        work2.primary_edition.published = today
        work2.license_pools[0].availability_time = None

        # This work has neither published nor issued. There will be no
        # dc:dateCopyrighted tag.
        work3 = self._work(with_open_access_download=True)
        work3.license_pools[0].availability_time = None

        self._db.commit()
        works = self._db.query(Work)
        with_times = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(with_times)
        assert 'dcterms:dateCopyrighted' in u
        with_times = feedparser.parse(u)
        e1, e2, e3 = sorted(
            with_times['entries'], key = lambda x: int(x['title']))

        eq_(the_past_s, e1['dcterms_datecopyrighted'])
        eq_(the_past_s, e1['published'])

        eq_(today_s, e2['dcterms_datecopyrighted'])
        assert not 'published' in e2

        assert not 'dcterms_datecopyrighted' in e3
        assert not 'published' in e3

    def test_acquisition_feed_includes_language_tag(self):
        work = self._work(with_open_access_download=True)
        work.primary_edition.publisher = "The Publisher"
        work2 = self._work(with_open_access_download=True)
        work2.primary_edition.publisher = None

        self._db.commit()

        works = self._db.query(Work)
        with_publisher = AcquisitionFeed(self._db, "test", "url", works)
        with_publisher = feedparser.parse(unicode(with_publisher))
        entries = sorted(with_publisher['entries'], key = lambda x: x['title'])
        eq_('The Publisher', entries[0]['dcterms_publisher'])
        assert 'publisher' not in entries[1]

    def test_acquisition_feed_includes_audience_tag(self):
        work = self._work(with_open_access_download=True)
        work.audience = "Young Adult"
        work2 = self._work(with_open_access_download=True)
        work2.audience = "Children"
        work3 = self._work(with_open_access_download=True)
        work3.audience = None

        self._db.commit()

        works = self._db.query(Work)
        with_publisher = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(with_publisher)
        with_publisher = feedparser.parse(u)
        entries = sorted(with_publisher['entries'], key = lambda x: int(x['title']))
        eq_("Young Adult", entries[0]['schema_name'])
        eq_("Children", entries[1]['schema_name'])
        assert not 'schema_name' in entries[2]

    def test_acquisition_feed_includes_category_tags_for_genres(self):
        work = self._work(with_open_access_download=True)
        g1, ignore = Genre.lookup(self._db, "Science Fiction")
        g2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [g1, g2]

        work2 = self._work(with_open_access_download=True)
        work2.genres = []
        work2.fiction = False

        work3 = self._work(with_open_access_download=True)
        work3.genres = []
        work3.fiction = True

        self._db.commit()
        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        eq_(['Romance', 'Science Fiction'], 
            sorted([x['term'] for x in entries[0]['tags']]))
        eq_(['Nonfiction'], [x['term'] for x in entries[1]['tags']])
        eq_(['Fiction'], [x['term'] for x in entries[2]['tags']])

    def test_acquisition_feed_includes_license_information(self):
        work = self._work(with_open_access_download=True)
        pool = work.license_pools[0]

        # These numbers are impossible, but it doesn't matter for
        # purposes of this test.
        pool.open_access = False
        pool.licenses_owned = 100
        pool.licenses_available = 50
        pool.patrons_in_hold_queue = 25
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(self._db, "test", "url", works)
        u = unicode(feed)
        feed = feedparser.parse(u)
        [entry] = feed['entries']
        eq_('100', entry['opds41_concurrent_lends'])
        eq_('50', entry['simplified_available_lends'])
        eq_('25', entry['simplified_active_holds'])

    def test_acquisition_feed_omits_works_with_no_active_license_pool(self):
        work = self._work(title="open access", with_open_access_download=True)
        no_license_pool = self._work(title="no license pool", with_license_pool=False)
        no_download = self._work(title="no download", with_license_pool=True)
        not_open_access = self._work("not open access", with_license_pool=True)
        not_open_access.license_pools[0].open_access = False
        self._db.commit()

        # We get a feed with only one entry--the one with an open-access
        # license pool and an associated download.
        works = self._db.query(Work)
        by_title = AcquisitionFeed(self._db, "test", "url", works)
        by_title = feedparser.parse(unicode(by_title))
        eq_(2, len(by_title['entries']))
        eq_(["not open access", "open access"], sorted(
            [x['title'] for x in by_title['entries']]))

    def test_featured_feed_ignores_low_quality_works(self):
        lane=self.lanes.by_name['Fantasy']
        good = self._work(genre=Fantasy, language="eng",
                          with_open_access_download=True)
        good.quality = 100
        bad = self._work(genre=Fantasy, language="eng",
                         with_open_access_download=True)
        bad.quality = 0

        # We get the good one and omit the bad one.
        feed = AcquisitionFeed.featured(self._db, "eng", lane)
        feed = feedparser.parse(unicode(feed))
        eq_([good.title], [x['title'] for x in feed['entries']])

    def test_active_loan_feed(self):
        patron = self.default_patron
        feed = AcquisitionFeed.active_loans_for(patron)
        # Nothing in the feed.
        feed = feedparser.parse(unicode(feed))
        eq_(0, len(feed['entries']))

        work = self._work(language="eng", with_open_access_download=True)
        work.license_pools[0].loan_to(patron)
        unused = self._work(language="eng", with_open_access_download=True)

        # Get the feed.
        feed = AcquisitionFeed.active_loans_for(patron)
        feed = feedparser.parse(unicode(feed))

        # The only entry in the feed is the work currently out on loan
        # to this patron.
        eq_(1, len(feed['entries']))
        eq_(work.title, feed['entries'][0]['title'])

