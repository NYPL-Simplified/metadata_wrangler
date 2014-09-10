import feedparser
from nose.tools import (
    eq_,
    set_trace,
)

from tests.db import (
    DatabaseTest,
)

from circulation import app

from model import (
    get_one_or_create,
    LaneList,
    Lane,
    Patron,
    Work,
)

from opds import (
    AcquisitionFeed,
    NavigationFeed,
    URLRewriter
)

from classifier import Classifier, Fantasy

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
         ]
        )

    
    def test_navigation_feed(self):
        original_feed = NavigationFeed.main_feed(self.lanes)
        parsed = feedparser.parse(unicode(original_feed))
        feed = parsed['feed']
        link = [link for link in feed['links'] if link['rel'] == 'self'][0]
        assert link['href'].endswith("/lanes/")

        # Every lane has an entry.
        eq_(3, len(parsed['entries']))
        tags = [x['title'] for x in parsed['entries']]
        eq_(['Fantasy', 'Fiction', 'Young Adult'], sorted(tags))

        # Let's take one entry as an example.
        toplevel = [x for x in parsed['entries'] if x.title == 'Fiction'][0]
        eq_("tag:Fiction", toplevel.id)

        # There are two links to acquisition feeds.
        self_link, featured, by_author = sorted(toplevel['links'])
        assert featured['href'].endswith("/lanes/Fiction")
        eq_("Featured", featured['title'])
        eq_(NavigationFeed.FEATURED_REL, featured['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, featured['type'])

        assert by_author['href'].endswith("/lanes/Fiction?order=author")
        eq_("All books", by_author['title'])
        eq_("subsection", by_author['rel'])
        eq_(NavigationFeed.ACQUISITION_FEED_TYPE, by_author['type'])

    def test_acquisition_feed(self):
        work = self._work(with_open_access_download=True)
        work.authors = "Alice"

        feed = AcquisitionFeed(self._db, "test", "http://the-url.com/",
                               [work])
        u = unicode(feed)
        parsed = feedparser.parse(u)
        [with_author] = parsed['entries']
        eq_("Alice", with_author['authors'][0]['name'])


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
                               works, facet_url_generator)
        u = unicode(feed)
        parsed = feedparser.parse(u)
        by_title = parsed['feed']

        alternate_link, self_link, by_author, by_title = sorted(
            by_title['links'])

        eq_("http://the-url.com/", self_link['href'])

        # As we'll see below, the feed parser parses facetGroup as
        # facetgroup; that's not a problem with the generator code.
        assert 'opds:facetgroup' not in u
        assert 'opds:facetGroup' in u

        eq_('Sort by', by_author['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_author['rel'])
        eq_('Author', by_author['title'])
        eq_(facet_url_generator("author"), by_author['href'])

        eq_('Sort by', by_title['opds:facetgroup'])
        eq_('http://opds-spec.org/facet', by_title['rel'])
        eq_('Title', by_title['title'])
        eq_(facet_url_generator("title"), by_title['href'])


    def test_acquisition_feed_includes_language_tag(self):
        work = self._work(with_open_access_download=True)
        work.languages = "eng,fre"
        work2 = self._work(with_open_access_download=True)
        work.languages = None
        self._db.commit()

        works = self._db.query(Work)
        with_language = AcquisitionFeed(self._db, "test", "url", works)
        with_language = feedparser.parse(unicode(with_language))
        entries = sorted(with_language['entries'], key = lambda x: x['title'])
        assert 'language' not in entries[0]
        eq_('en', entries[1]['dcterms_language'])


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

