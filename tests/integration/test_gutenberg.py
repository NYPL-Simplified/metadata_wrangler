# encoding: utf-8

import datetime
import pkgutil
import StringIO
from nose.tools import set_trace, eq_ 
from model import (
    Author,
    DataSource,
    SubjectType,
    WorkIdentifier,
    WorkRecord,
    get_one_or_create,
)
from integration.gutenberg import (
    GutenbergAPI,
    GutenbergRDFExtractor,
)

from tests.db import (
    setup_module,
    teardown_module,
    DatabaseTest,
)

class TestGutenbergAPI(DatabaseTest):

    def test_pg_license_is_open_access(self):

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier, ignore = get_one_or_create(
            self._db, WorkIdentifier, type=WorkIdentifier.GUTENBERG_ID,
            identifier="17")        
        work_record, new = WorkRecord.for_foreign_id(
            self._db, DataSource.GUTENBERG, WorkIdentifier.GUTENBERG_ID, "1")
        eq_(True, new)

        license, new = GutenbergAPI.pg_license_for(self._db, work_record)
        eq_(True, new)
        eq_(True, license.open_access)

class TestGutenbergMetadataExtractor(DatabaseTest):

    def test_rdf_parser(self):
        """Parse RDF into a WorkRecord."""
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-17.rdf"))
        book, new = GutenbergRDFExtractor.book_in(self._db, "17", fh)

        # Verify that the WorkRecord is hooked up to the correct
        # DataSource and WorkIdentifier.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier, ignore = get_one_or_create(
            self._db, WorkIdentifier, type=WorkIdentifier.GUTENBERG_ID,
            identifier="17")
        eq_(gutenberg, book.data_source)
        eq_(identifier, book.primary_identifier)

        eq_(["http://www.gutenberg.org/ebooks/17"], 
            [str(x['href']) for x in book.links['canonical']])

        eq_("The Book of Mormon", book.title)
        eq_("An Account Written by the Hand of Mormon Upon Plates Taken from the Plates of Nephi", book.subtitle)

        eq_("Project Gutenberg", book.publisher)
        eq_(["en"], book.languages)

        eq_(datetime.date(2008, 6, 25), book.issued)

        a1, a2 = sorted(book.authors, key = lambda x: x[Author.NAME])
        eq_("Church of Jesus Christ of Latter-day Saints", a1[Author.NAME])
        assert Author.ALTERNATE_NAME not in a1
        assert Author.ROLE not in a2

        eq_("Smith, Joseph, Jr.", a2[Author.NAME])
        eq_(["Smith, Joseph"], a2[Author.ALTERNATE_NAME])
        assert 'role' not in a2

        # The book has a LCC classification...
        subjects = book.subjects
        [lcc] = subjects[SubjectType.LCC]
        eq_("BX", lcc['id'])

        # ...and two LCSH classifications
        lcsh = subjects[SubjectType.LCSH]
        eq_([u'Church of Jesus Christ of Latter-day Saints -- Sacred books',
             u'Mormon Church -- Sacred books'], 
            sorted(x['id'] for x in lcsh))

    def test_unicode_characters_in_title(self):
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-10130.rdf"))
        book, new = GutenbergRDFExtractor.book_in(self._db, "10130", fh)
        eq_(u"The Works of Charles and Mary Lamb — Volume 3", book.title)
        eq_("Books for Children", book.subtitle)

    def test_rdf_file_describing_no_books(self):
        """GutenbergRDFExtractor can handle an RDF document that doesn't
        describe any books."""
        fh = StringIO.StringIO(pkgutil.get_data(
            "tests.integration",
            "files/gutenberg-0.rdf"))
        book, new = GutenbergRDFExtractor.book_in(self._db, "0", fh)
        eq_(None, book)
        eq_(False, new)
