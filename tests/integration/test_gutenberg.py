# encoding: utf-8

import datetime
import pkgutil
import StringIO
from nose.tools import set_trace, eq_ 
from model import (
    get_one_or_create,
    DataSource,
    WorkRecord,
    WorkIdentifier,
    Author,
)
from integration.gutenberg import (
    GutenbergAPI,
    GutenbergRDFExtractor,
)

from tests.test_model import (
    setup_module,
    teardown_module,
    DatabaseTest,
)

class TestGutenbergMetadataExtractor(DatabaseTest):

    def test_rdf_parser(self):
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-17.rdf"))
        [[book, license_pool]] = GutenbergRDFExtractor.books_in(self._db, "17", fh)

        # Verify that the data model is hooked up correctly.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier, ignore = get_one_or_create(
            self._db, WorkIdentifier, type=WorkIdentifier.GUTENBERG_ID,
            identifier="17")
        eq_(gutenberg, license_pool.data_source)
        eq_(identifier, license_pool.identifier)
        eq_(gutenberg, book.data_source)
        eq_(identifier, book.primary_identifier)

        # TODO: verify that the canonical link is in place.
        d = dict(rel="canonical", href="http://www.gutenberg.org/ebooks/17")
        #assert d in book.links
        eq_("The Book of Mormon", book.title)
        eq_("An Account Written by the Hand of Mormon Upon Plates Taken from the Plates of Nephi", book.subtitle)

        eq_("Project Gutenberg", book.publisher)
        eq_(["en"], book.languages)

        eq_(datetime.date(2008, 6, 25), book.issued)

        # TODO: I left off here.
        a1, a2 = sorted(book[Edition.AUTHOR], key = lambda x: x['name'])
        eq_("Church of Jesus Christ of Latter-day Saints", a1[Author.NAME])
        assert Author.ALTERNATE_NAME not in a1
        eq_("Smith, Joseph, Jr.", a2[Author.NAME])
        eq_(["Smith, Joseph"], a2[Author.ALTERNATE_NAME])

        # The book has a LCC classification...
        subject = book[Edition.SUBJECT]
        lcc = subject[GutenbergRDFExtractor.lcc_vocabulary]
        eq_(["BX"], lcc)

        # ...and two LCSH classifications
        lcsh = subject[GutenbergRDFExtractor.lcsh_vocabulary]
        eq_([u'Church of Jesus Christ of Latter-day Saints -- Sacred books',
             u'Mormon Church -- Sacred books'], sorted(lcsh))

    def test_unicode_characters_in_title(self):
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-10130.rdf"))
        [[book, license_pool]] = list(GutenbergRDFExtractor.books_in(self._db, "10130", fh))
        eq_(u"The Works of Charles and Mary Lamb — Volume 3", book.title)
        eq_("Books for Children", book.subtitle)
