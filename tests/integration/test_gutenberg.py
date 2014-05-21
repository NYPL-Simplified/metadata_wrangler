# encoding: utf-8

import datetime
import pkgutil
import StringIO
from nose.tools import set_trace, eq_ 
from model import (
    Author,
)
from integration.gutenberg import (
    GutenbergAPI,
    GutenbergRDFExtractor,
)

class TestGutenbergMetadataExtractor(object):

    def test_rdf_parser(self):
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-17.rdf"))
        [book] = list(GutenbergRDFExtractor.books_in(fh))
        set_Trace()
        eq_(GutenbergAPI.EVENT_SOURCE, book[Edition.SOURCE])
        eq_("17", book[Edition.SOURCE_ID])
        eq_("http://www.gutenberg.org/ebooks/17", book["uri"])
        eq_("The Book of Mormon", book[Edition.TITLE])
        eq_("An Account Written by the Hand of Mormon Upon Plates Taken from the Plates of Nephi", book[Edition.SUBTITLE])

        eq_("Project Gutenberg", book[Edition.PUBLISHER])
        eq_(["en"], book[Edition.LANGUAGE])

        assert isinstance(book[Edition.DATE_ISSUED], datetime.datetime)

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
        [book] = list(GutenbergRDFExtractor.books_in(fh))
        eq_(u"The Works of Charles and Mary Lamb — Volume 3", book[Edition.TITLE])
        eq_("Books for Children", book[Edition.SUBTITLE])
