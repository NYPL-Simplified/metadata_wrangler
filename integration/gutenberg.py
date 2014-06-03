import datetime
import os
import json
import random
import re
import requests
import time
import shutil
import tarfile
from StringIO import StringIO

from sqlalchemy.orm import aliased

from nose.tools import set_trace

import rdflib
from rdflib import Namespace

from model import (
    get_one_or_create,
    CirculationEvent,
    WorkRecord,
    DataSource,
    WorkIdentifier,
    LicensePool,
    SubjectType,
)

from monitor import Monitor
from integration.oclc import (
    OCLCClassifyAPI,
    OCLCXMLParser,
)
from util import LanguageCodes

class GutenbergAPI(object):

    """An 'API' to Project Gutenberg's RDF catalog.

    A bit different from the other APIs since the data comes over the
    web all at once in one big BZ2 file.
    """

    ID_IN_FILENAME = re.compile("pg([0-9]+).rdf")

    EVENT_SOURCE = "Gutenberg"
    FILENAME = "rdf-files.tar.bz2"

    ONE_DAY = 60 * 60 * 24

    MIRRORS = [
        "http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2",
        "http://gutenberg.readingroo.ms/cache/generated/feeds/rdf-files.tar.bz2",
        "http://snowy.arsc.alaska.edu/gutenberg/cache/generated/feeds/rdf-files.tar.bz2",        
    ] 

    def __init__(self, data_directory):
        self.data_directory = data_directory
        self.catalog_path = os.path.join(self.data_directory, self.FILENAME)

    def update_catalog(self):
        """Download the most recent Project Gutenberg catalog
        from a randomly selected mirror."""
        url = random.choice(self.MIRRORS)
        print "Refreshing %s" % url
        data = requests.get(url)
        tmp_path = self.catalog_path + ".tmp"
        open(tmp_path, "wb").write(data.content)
        shutil.move(tmp_path, self.catalog_path)

    def needs_refresh(self):
        """Is it time to download a new version of the catalog?"""
        if os.path.exists(self.catalog_path):
            modification_time = os.stat(self.catalog_path).st_mtime
            return (time.time() - modification_time) >= self.ONE_DAY
        return True

    def all_books(self):
        """Yields raw data for every book in the PG catalog."""
        if self.needs_refresh():
            self.update_catalog()
        archive = tarfile.open(self.catalog_path)
        next_item = archive.next()
        a = 0
        while next_item:
            if next_item.isfile() and next_item.name.endswith(".rdf"):
                pg_id = self.ID_IN_FILENAME.search(next_item.name).groups()[0]
                yield pg_id, archive, next_item
            next_item = archive.next()

    def create_missing_books(self, _db):
        """Finds books present in the PG catalog but missing from WorkRecord.

        Yields (WorkRecord, LicensePool) 2-tuples.
        """
        # TODO: We are temporarily using a very limited dataset
        only_import = set(map(str, [11, 19033, 28885, 928, 19778, 19597, 28371, 17482, 23716, 114, 19002, 10643, 36308, 19551, 35688, 35990, 2701, 15, 2489, 28794, 9147, 76, 32325, 19640, 9007, 7100, 7101, 7102, 7103, 7104, 7105, 7106, 7107, 74, 30165, 26203, 93, 7193, 91, 7194, 7198, 9038, 7195, 30890, 7196, 7197, 45333, 7199, 7200, 9037, 9036, 12, 23718]))
        books = self.all_books()
        source = DataSource.GUTENBERG
        for pg_id, archive, archive_item in books:
            if int(pg_id) > 10000:
                continue
            #if pg_id not in only_import:
            #    continue
            print "Considering %s" % pg_id

            # Find an existing WorkRecord for the book.
            book = WorkRecord.for_foreign_id(
                _db, source, WorkIdentifier.GUTENBERG_ID, pg_id,
                create_if_not_exists=False)

            if not book:
                # Create a new WorkRecord object with bibliographic
                # information from the Project Gutenberg RDF file.
                print "%s is new." % pg_id
                fh = archive.extractfile(archive_item)
                data = fh.read()
                fake_fh = StringIO(data)
                book, new = GutenbergRDFExtractor.book_in(_db, pg_id, fake_fh)

            if book:
                # Ensure that an open-access LicensePool exists for this book.
                license, new = self.pg_license_for(_db, book)
                yield (book, license)

    @classmethod
    def pg_license_for(cls, _db, work_record):
        """Retrieve a LicensePool for the given Project Gutenberg work,
        creating it (but not committing it) if necessary.
        """
        return get_one_or_create(
            _db, LicensePool,
            data_source=work_record.data_source,
            identifier=work_record.primary_identifier,
            create_method_kwargs=dict(
                open_access=True,
                last_checked=datetime.datetime.now(),
            )
        )

 
class GutenbergRDFExtractor(object):

    """Transform a Project Gutenberg RDF description of a title into a
    WorkRecord object and an open-access LicensePool object.
    """

    dcterms = Namespace("http://purl.org/dc/terms/")
    dcam = Namespace("http://purl.org/dc/dcam/")
    rdf = Namespace(u'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    gutenberg = Namespace("http://www.gutenberg.org/2009/pgterms/")

    ID_IN_URI = re.compile("/([0-9]+)$")

    FORMAT = "format"

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def _values(cls, graph, query):
        """Return just the values of subject-predicate-value triples."""
        return [x[2] for x in graph.triples(query)]

    @classmethod
    def _value(cls, graph, query):
        """Return just one value for a subject-predicate-value triple."""
        v = cls._values(graph, query)
        if v:
            return v[0]
        return None

    @classmethod
    def book_in(cls, _db, pg_id, fh):

        """Yield a WorkRecord object for the book described by the given
        filehandle, creating it (but not committing it) if necessary.

        This assumes that there is at most one book per
        filehandle--the one identified by ``pg_id``. However, a file
        may turn out to describe no books at all (such as pg_id=1984,
        reserved for George Orwell's "1984"). In that case,
        ``book_in()`` will return None.
        """
        g = rdflib.Graph()
        g.load(fh)
        data = dict()

        # Determine the 'about' URI.
        title_triples = list(g.triples((None, cls.dcterms['title'], None)))

        book = None
        new = False
        if title_triples:
            if len(title_triples) > 1:
                # Each filehandle is associated with one Project Gutenberg ID 
                # and should thus contain at most one title.
                raise ValueError(
                    "More than one title associated with Project Gutenberg ID %s" % pg_id)
            uri, ignore, title = title_triples[0]
            book, new = cls.parse_book(_db, g, uri, title)
        return book, new

    @classmethod
    def parse_book(cls, _db, g, uri, title):
        """Turn an RDF graph into a WorkRecord for the given `uri` and
        `title`.
        """
        source_id = unicode(cls.ID_IN_URI.search(uri).groups()[0])
        # Split a subtitle out from the main title.
        title = unicode(title)
        subtitle = None
        for separator in "\r\n", "\n":
            if separator in title:
                parts = title.split(separator)
                title = parts[0]
                subtitle = "\n".join(parts[1:])
                break
        print " %s" % title

        issued = cls._value(g, (uri, cls.dcterms.issued, None))
        issued = datetime.datetime.strptime(issued, cls.DATE_FORMAT).date()

        summary = cls._value(g, (uri, cls.dcterms.description, None))
        summary = WorkRecord._content(summary)
        
        publisher = cls._value(g, (uri, cls.dcterms.publisher, None))

        languages = []
        for ignore, ignore, language_uri in g.triples(
                (uri, cls.dcterms.language, None)):
            code = str(cls._value(g, (language_uri, cls.rdf.value, None)))
            code = LanguageCodes.two_to_three[code]
            if code:
                languages.append(code)

        links = dict(canonical=[dict(href=uri)])
        download_links = cls._values(g, (uri, cls.dcterms.hasFormat, None))
        for href in download_links:
            for format_uri in cls._values(
                    g, (href, cls.dcterms['format'], None)):
                media_type = cls._value(g, (format_uri, cls.rdf.value, None))
                WorkRecord._add_link(
                    links, WorkRecord.OPEN_ACCESS_DOWNLOAD, href, media_type)
        
        subjects = dict()
        subject_links = cls._values(g, (uri, cls.dcterms.subject, None))
        for subject in subject_links:
            value = cls._value(g, (subject, cls.rdf.value, None))
            vocabulary = cls._value(g, (subject, cls.dcam.memberOf, None))
            vocabulary=SubjectType.by_uri[str(vocabulary)]
            WorkRecord._add_subject(subjects, vocabulary, value)

        authors = []
        for ignore, ignore, author_uri in g.triples((uri, cls.dcterms.creator, None)):
            name = cls._value(g, (author_uri, cls.gutenberg.name, None))
            aliases = cls._values(g, (author_uri, cls.gutenberg.alias, None))
            WorkRecord._add_author(authors, name, aliases=aliases)

        # Create or fetch a WorkRecord for this book.
        source = DataSource.lookup(_db, DataSource.GUTENBERG)
        identifier, new = WorkIdentifier.for_foreign_id(
            _db, WorkIdentifier.GUTENBERG_ID, source_id)
        book, new = get_one_or_create(
            _db, WorkRecord,
            create_method_kwargs=dict(
                title=title,
                subtitle=subtitle,
                issued=issued,
                summary=summary,
                publisher=publisher,
                languages=languages,
                links=links,
                subjects=subjects,
                authors=authors),
            data_source=source,
            primary_identifier=identifier,
        )

        return book, new


class GutenbergMonitor(Monitor):
    """Maintain license pool and metadata info for Gutenberg titles.
    """

    def __init__(self, data_directory):
        path = os.path.join(data_directory, DataSource.GUTENBERG)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = GutenbergAPI(path)

    def run(self, _db):
        added_books = 0
        for work, license_pool in self.source.create_missing_books(_db):
            # Log a circulation event for this work.
            event = get_one_or_create(
                _db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked
                )
            )
            _db.commit()


class OCLCMonitorForGutenberg(object):

    """Track OCLC's opinions about books with the same title/author as 
    Gutenberg works."""

    # Strips most non-alphanumerics from the title.
    # 'Alphanumerics' includes alphanumeric characters
    # for any language, so this shouldn't affect
    # titles in non-Latin languages.
    #
    # OCLC has trouble recognizing non-alphanumerics in titles,
    # especially colons.
    NON_TITLE_SAFE = re.compile("[^\w\-' ]", re.UNICODE)
    
    def __init__(self, data_directory):
        self.gutenberg = GutenbergMonitor(data_directory)
        self.oclc = OCLCClassifyAPI(data_directory)

    def oclc_safe_title(self, title):
        return self.NON_TITLE_SAFE.sub("", title)

    def title_and_author(self, book):
        title = self.oclc_safe_title(book.title)

        authors = book.authors
        if len(authors) == 0:
            author = ''
        else:
            author = authors[0]['name']
        return title, author

    def run(self, _db):
        counter = 0

        in_gutenberg_but_not_in_oclc = WorkRecord.missing_coverage_from(
            _db, WorkIdentifier.GUTENBERG_ID,
            WorkIdentifier.OCLC_WORK, WorkIdentifier.OCLC_NUMBER)

        for book in in_gutenberg_but_not_in_oclc:
            title, author = self.title_and_author(book)
            languages = book.languages

            print '%s "%s" "%s" %r' % (book.primary_identifier.identifier, title, author, languages)
            # Perform a title/author lookup
            xml = self.oclc.lookup_by(title=title, author=author)

            restrictions = dict(
                title=title, languages=languages, authors=[author])

            # Turn the raw XML into some number of bibliographic records.
            representation_type, records = OCLCXMLParser.parse(
                _db, xml, **restrictions)

            if representation_type == OCLCXMLParser.MULTI_WORK_STATUS:
                # `records` contains a bunch of SWIDs, not
                # WorkRecords. Do another lookup to turn each SWID
                # into a set of WorkRecords.
                swids = records
                records = []
                for swid in swids:
                    xml = self.oclc.lookup_by(swid=swid)
                    representation_type, editions = OCLCXMLParser.parse(
                        _db, xml, **restrictions)

                    if representation_type == OCLCXMLParser.SINGLE_WORK_DETAIL_STATUS:
                        records.extend(editions)
                    else:
                        print " Got unexpected representation type from lookup: %s" % representation_type
            # Connect the Gutenberg book to the OCLC works looked up by
            # title/author.
            book.equivalent_identifiers.extend(
                [r.primary_identifier for r in records])

            print " Created %s records(s)." % len(records)
            _db.commit()
        _db.commit()
