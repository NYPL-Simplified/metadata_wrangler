import datetime
import os
import json
import random
import re
import random
import requests
import time
import shutil
import tarfile
from urlparse import urljoin, urlparse
from StringIO import StringIO
from bs4 import BeautifulSoup

from sqlalchemy.orm import aliased

from nose.tools import set_trace

import rdflib
from rdflib import Namespace

from model import (
    get_one_or_create,
    CirculationEvent,
    CoverageProvider,
    Contributor,
    Edition,
    DataSource,
    Measurement,
    Representation,
    Resource,
    Identifier,
    LicensePool,
    Subject,
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
    ] 

    # This will be passed in to Representation.get when downloading
    # the mirror.
    def http_get_from_random_mirror(self, url, headers):
        actual_url = random.choice(MIRRORS)
        return Representations.simple_http_get(actual_url, headers)

    GUTENBERG_ORIGINAL_MIRROR = "%(gutenberg_original_mirror)s"
    GUTENBERG_EBOOK_MIRROR = "%(gutenberg_ebook_mirror)s"
    EPUB_ID = re.compile("/([0-9]+)")

    def __init__(self, _db, data_directory):
        self._db = _db
        self.source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.data_directory = data_directory
        self.catalog_path = os.path.join(self.data_directory, self.FILENAME)

    @classmethod
    def url_to_mirror_path(cls, url):
        """Convert a URL as seen in the RDF file to the corresponding
        path in the local archive of a Gutenberg mirror.
        """
        parsed = urlparse(url)
        if parsed.hostname not in ('www.gutenberg.org', 'gutenberg.org'):
            raise ValueError("Not a Gutenberg URL: %s" % url)

        mirror = cls.GUTENBERG_ORIGINAL_MIRROR
        if parsed.path.startswith('/files/'):
            # /files/8594/8594.txt
            #     =>
            # /8/5/9/8594/8494.txt           
            text_id = cls.EPUB_ID.search(parsed.path).groups()[0]
            path_parts = [i for i in text_id[:-1]]
            new_path_prefix = os.path.join(*path_parts) 
            new_path = parsed.path.replace(
                '/files/', "/" + new_path_prefix + "/", 1)
            mirror = cls.GUTENBERG_ORIGINAL_MIRROR
        elif parsed.path.startswith('/dirs/'):
            # /dirs/etext05/8ivn110.zip
            #     =>
            # /etext05/8ivn110.zip
            mirror = cls.GUTENBERG_ORIGINAL_MIRROR
            new_path = parsed.path.replace('/dirs/', '/', 1)
        elif parsed.path.startswith('/ebooks/'):
            mirror = cls.GUTENBERG_EBOOK_MIRROR
            text_id = cls.EPUB_ID.search(parsed.path).groups()[0]
            if '.epub' in parsed.path:
                # /ebooks/8594.epub.images
                #     =>
                # /8594/pg8594-images.epub
                #
                # /ebooks/8605.epub.noimages
                #     =>
                # /8605/pg8605.epub
                if 'noimages' in parsed.path:
                    new_path = "/%(text_id)s/pg%(text_id)s.epub" 
                else:
                    new_path = "/%(text_id)s/pg%(text_id)s-images.epub"
                new_path = new_path % dict(text_id=text_id)
            else:
                # /ebooks/8596.plucker
                #     =>
                # /8596/pg8596.plucker
                new_path = parsed.path.replace(
                    "/ebooks/", "/" + text_id + "/pg")
        elif parsed.path.startswith('/cache/epub/'):
            mirror = cls.GUTENBERG_EBOOK_MIRROR
            # /cache/epub/38044/pg38044.cover.medium.jpg 
            #     =>
            # /38044/pg38044.cover.medium.jpg 
            new_path = parsed.path.replace('/cache/epub/', '/', 1)

        return mirror, new_path

    def update_catalog(self):
        """Download the most recent Project Gutenberg catalog
        from a randomly selected mirror.

        We don't use Representation (for now) because the file is huge
        and the tarfile module only supports reading from a file on
        disk.
        """
        url = random.choice(self.MIRRORS)
        print "Refreshing %s" % url
        response = requests.get(url)
        tmp_path = self.catalog_path + ".tmp"
        open(tmp_path, "wb").write(response.content)
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

    def create_missing_books(self, subset=None):
        """Finds books present in the PG catalog but missing from Edition.

        Yields (Edition, LicensePool) 2-tuples.
        """
        books = self.all_books()
        for pg_id, archive, archive_item in books:
            if subset is not None and not subset(pg_id, archive, archive_item):
                continue
            print "Considering %s" % pg_id
            # Find an existing Edition for the book.
            book = Edition.for_foreign_id(
                self._db, self.source, Identifier.GUTENBERG_ID, pg_id,
                create_if_not_exists=False)

            if not book:
                # Create a new Edition object with bibliographic
                # information from the Project Gutenberg RDF file.
                fh = archive.extractfile(archive_item)
                data = fh.read()
                fake_fh = StringIO(data)
                book, new = GutenbergRDFExtractor.book_in(
                    self._db, pg_id, fake_fh)

                if book:
                    # Ensure that an open-access LicensePool exists for this book.
                    license, new = self.pg_license_for(self._db, book)
                    yield (book, license)

    @classmethod
    def pg_license_for(cls, _db, edition):
        """Retrieve a LicensePool for the given Project Gutenberg work,
        creating it (but not committing it) if necessary.
        """
        return get_one_or_create(
            _db, LicensePool,
            data_source=edition.data_source,
            identifier=edition.primary_identifier,
            create_method_kwargs=dict(
                open_access=True,
                last_checked=datetime.datetime.now(),
            )
        )

 
class GutenbergRDFExtractor(object):

    """Transform a Project Gutenberg RDF description of a title into a
    Edition object and an open-access LicensePool object.
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

        """Yield a Edition object for the book described by the given
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

        new = False
        if title_triples:
            if len(title_triples) > 1:
                uris = set([x[0] for x in title_triples])
                if len(uris) > 1:
                    # Each filehandle is associated with one Project
                    # Gutenberg ID and should thus describe at most
                    # one title.
                    raise ValueError(
                        "More than one book in file for Project Gutenberg ID %s" % pg_id)
                else:
                    print "WEIRD MULTI-TITLE: %s" % pg_id

            # TODO: Some titles such as 44244 have titles in multiple
            # languages. Not sure what to do about that.
            uri, ignore, title = title_triples[0]
            print " Parsing book %s" % title
            book, new = cls.parse_book(_db, g, uri, title)
        else:
            book = None
            new = False
        return book, new

    @classmethod
    def parse_book(cls, _db, g, uri, title):
        """Turn an RDF graph into a Edition for the given `uri` and
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

        issued = cls._value(g, (uri, cls.dcterms.issued, None))
        issued = datetime.datetime.strptime(issued, cls.DATE_FORMAT).date()

        # As far as I can tell, Gutenberg descriptions are 100%
        # useless for our purposes. They should not be used, even if
        # no other description is available.
        # summary = cls._value(g, (uri, cls.dcterms.description, None))
        summary = None

        publisher = cls._value(g, (uri, cls.dcterms.publisher, None))

        languages = []
        for ignore, ignore, language_uri in g.triples(
                (uri, cls.dcterms.language, None)):
            code = str(cls._value(g, (language_uri, cls.rdf.value, None)))
            code = LanguageCodes.two_to_three[code]
            if code:
                languages.append(code)

        if 'eng' in languages:
            language = 'eng'
        elif languages:
            language = languages[0]
        else:
            language = None

        contributors = []
        for ignore, ignore, author_uri in g.triples((uri, cls.dcterms.creator, None)):
            name = cls._value(g, (author_uri, cls.gutenberg.name, None))
            aliases = cls._values(g, (author_uri, cls.gutenberg.alias, None))
            matches, new = Contributor.lookup(_db, name, aliases=aliases)
            contributor = matches[0]
            contributors.append(contributor)

        # Create or fetch a Edition for this book.
        source = DataSource.lookup(_db, DataSource.GUTENBERG)
        identifier, new = Identifier.for_foreign_id(
            _db, Identifier.GUTENBERG_ID, source_id)
        book, new = get_one_or_create(
            _db, Edition,
            create_method_kwargs=dict(
                title=title,
                subtitle=subtitle,
                issued=issued,
                publisher=publisher,
                language=language,
            ),
            data_source=source,
            primary_identifier=identifier,
        )

        # Classify the Edition.
        if new:
            subject_links = cls._values(g, (uri, cls.dcterms.subject, None))
            for subject in subject_links:
                value = cls._value(g, (subject, cls.rdf.value, None))
                vocabulary = cls._value(g, (subject, cls.dcam.memberOf, None))
                vocabulary = Subject.by_uri[str(vocabulary)]
                identifier.classify(source, vocabulary, value)


        # If there is a description, turn it into a Resource.
        identifier = book.primary_identifier
        if summary:
            rel = Resource.DESCRIPTION
            identifier.add_resource(
                rel, None, source, media_type="text/plain", content=summary)

        medium = Edition.BOOK_MEDIUM

        # Turn the Gutenberg download links into Resources associated 
        # with the new Edition. They will serve either as open access
        # downloads or cover images.
        download_links = cls._values(g, (uri, cls.dcterms.hasFormat, None))
        for href in download_links:
            for format_uri in cls._values(
                    g, (href, cls.dcterms['format'], None)):
                media_type = unicode(
                    cls._value(g, (format_uri, cls.rdf.value, None)))
                rel = Resource.OPEN_ACCESS_DOWNLOAD
                if media_type.startswith('image/'):
                    if '.medium.' in href:
                        rel = Resource.IMAGE
                    else:
                        # We don't care about thumbnail images--we
                        # make our own.
                        rel = None
                elif media_type.startswith('audio/'):
                    medium = Edition.AUDIO_MEDIUM
                elif media_type.startswith('video/'):
                    medium = Edition.VIDEO_MEDIUM
                if rel:
                    identifier.add_resource(
                        rel, unicode(href), source, media_type=media_type)
                identifier.add_resource(
                    Resource.CANONICAL, unicode(uri), source)

        book.medium = medium

        # Associate the appropriate contributors with the book.
        for contributor in contributors:
            book.add_contributor(contributor, Contributor.AUTHOR_ROLE)
        return book, new


class GutenbergMonitor(Monitor):
    """Maintain license pool and metadata info for Gutenberg titles.
    """

    def __init__(self, _db, data_directory):
        self._db = _db
        path = os.path.join(data_directory, DataSource.GUTENBERG)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = GutenbergAPI(_db, path)

    def run(self, subset=None):
        added_books = 0
        for work, license_pool in self.source.create_missing_books(subset):
            # Log a circulation event for this work.
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked
                )
            )
            self._db.commit()


class OCLCMonitorForGutenberg(CoverageProvider):

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
    
    def __init__(self, _db, data_directory):
        self._db = _db
        self.gutenberg = GutenbergMonitor(self._db, data_directory)
        self.oclc_classify = OCLCClassifyAPI(self._db)
        input_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        output_source = DataSource.lookup(self._db, DataSource.OCLC)
        super(OCLCMonitorForGutenberg, self).__init__(
            "OCLC Monitor for Gutenberg", input_source, output_source)

    def oclc_safe_title(self, title):
        return self.NON_TITLE_SAFE.sub("", title)

    def title_and_author(self, book):
        title = self.oclc_safe_title(book.title)

        authors = book.author_contributors
        if len(authors) == 0:
            author = ''
        else:
            author = authors[0].name
        return title, author

    def process_edition(self, book):
        title, author = self.title_and_author(book)
        language = book.language

        print '%s "%s" "%s" %r' % (book.primary_identifier.identifier, title, author, language)
        # Perform a title/author lookup
        xml = self.oclc_classify.lookup_by(title=title, author=author)

        # For now, the only restriction we apply is the language
        # restriction. If we know that a given OCLC record is in a
        # different language from this record, there's no need to
        # even import that record. Restrictions on title and
        # author will be applied statistically, when we calculate
        # works.
        restrictions = dict(language=language,
                            title=title,
                            authors=book.author_contributors)

        # Turn the raw XML into some number of bibliographic records.
        representation_type, records = OCLCXMLParser.parse(
            self._db, xml, **restrictions)

        if representation_type == OCLCXMLParser.MULTI_WORK_STATUS:
            # `records` contains a bunch of SWIDs, not
            # Editions. Do another lookup to turn each SWID
            # into a set of Editions.
            swids = records
            records = []
            for swid in swids:
                swid_xml = self.oclc_classify.lookup_by(swid=swid)
                representation_type, editions = OCLCXMLParser.parse(
                    self._db, swid_xml, **restrictions)

                if representation_type == OCLCXMLParser.SINGLE_WORK_DETAIL_STATUS:
                    records.extend(editions)
                elif representation_type == OCLCXMLParser.NOT_FOUND_STATUS:
                    # This shouldn't happen, but if it does,
                    # it's not a big deal. Just do nothing.
                    pass
                elif representation_type == OCLCXMLParser.INVALID_INPUT_STATUS:
                    # This also shouldn't happen, but if it does,
                    # there's nothing we can do.
                    pass                    
                else:
                    print " Got unexpected representation type from lookup: %s" % representation_type
        # Connect the Gutenberg book to the OCLC works looked up by
        # title/author. Hopefully we can also connect the Gutenberg book
        # to an author who has an LC and VIAF.

        # First, find any authors associated with this book that
        # have not been given VIAF or LC IDs.
        gutenberg_authors_to_merge = [
            x for x in book.author_contributors if not x.viaf or not x.lc
        ]
        gutenberg_names = set([x.name for x in book.author_contributors])
        for r in records:
            if gutenberg_authors_to_merge:
                oclc_names = set([x.name for x in r.author_contributors])
                if gutenberg_names == oclc_names:
                    # Perfect overlap. We've found an OCLC record
                    # for a book written by exactly the same
                    # people as the Gutenberg book. Merge each
                    # Gutenberg author into its OCLC equivalent.
                    for gutenberg_author in gutenberg_authors_to_merge:
                        oclc_authors = [x for x in r.author_contributors
                                        if x.name==gutenberg_author.name]
                        if len(oclc_authors) == 1:
                            oclc_author = oclc_authors[0]
                            if oclc_author != gutenberg_author:
                                gutenberg_author.merge_into(oclc_author)
                                gutenberg_authors_to_merge.remove(
                                    gutenberg_author)

            # Now that we've (perhaps) merged authors, calculate the
            # similarity between the two records.
            strength = book.similarity_to(r)
            if strength > 0:
                book.primary_identifier.equivalent_to(
                    self.output_source, r.primary_identifier, strength)

        print " Created %s records(s)." % len(records)
        return True

class GutenbergBookshelfClient(object):
    """Get classifications and measurements of popularity from Gutenberg
    bookshelves.
    """

    BASE_URL = "http://www.gutenberg.org/wiki/Category:Bookshelf"
    gutenberg_text_number = re.compile("/ebooks/([0-9]+)")
    number_of_downloads = re.compile("([0-9]+) download")

    def __init__(self, _db):
        self._db = _db
        self.data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

    def do_get_with_captcha_trapdoor(self, *args, **kwargs):
        kwargs['proxies'] = dict(http='http://us-il.proxymesh.com:31280')
        status_code, headers, content = Representation.browser_http_get(*args, **kwargs)
        if 'captcha' in content:
            raise IOError("Triggered CAPTCHA.")
        return status_code, headers, content

    def do_get(self, referer, url, handled):
        headers = dict()
        if referer:
            headers['Referer'] = referer
        if not url.startswith('http'):
            url = urljoin(self.BASE_URL, url)
        if url in handled:
            return None
        representation, cached = Representation.get(
            self._db, url, self.do_get_with_captcha_trapdoor,
            headers, data_source=self.data_source,
            pause_before=random.random()*5)
        if not cached:
            self._db.commit()
        handled.add(url)
        return representation

    def full_update(self):
        all_classifications = dict()
        all_favorites = set()
        all_download_counts = dict()

        url = self.BASE_URL
        lists_of_shelves = [(None, url)]
        shelves = []
        handled = set()
        while lists_of_shelves:
            referer, url = lists_of_shelves.pop()
            representation = self.do_get(referer, url, handled)
            if not representation:
                # Already handled
                continue
            new_lists, new_shelves = self.process_bookshelf_list_page(
                representation)
            for i in new_lists:
                lists_of_shelves.append((url, i))
            for shelf_url, shelf_name in new_shelves:
                shelves.append((url, shelf_url, shelf_name))

        # Now get the contents of each bookshelf.
        for referer, url, bookshelf_name in shelves:
            representation = self.do_get(referer, url, handled) 
            if not representation:
                # Already handled
                continue
            texts, favorites, downloads = self.process_shelf(
                representation, handled)
            all_classifications[bookshelf_name] = texts
            all_favorites = all_favorites.union(favorites)
            all_download_counts.update(downloads)
            print "%s %d %d %d %d %d" % (
                bookshelf_name, len(favorites), len(all_favorites), 
                len(downloads), len(all_download_counts),
                len(texts))
        set_trace()
        self.set_favorites(all_favorites)
        self.set_download_counts(all_download_counts)
        self.classify(all_classifications)

    def _title(self, identifier):
        a = identifier.primarily_identifies
        if not a:
            return "(unknown)"
        else:
            return a[0].title

    def _gutenberg_id_lookup(self, ids):
        return self._db.query(Identifier).filter(
            Identifier.identifier.in_(ids)).filter(
                Identifier.type==Identifier.GUTENBERG_ID)

    def set_favorites(self, ids):
        # TODO: Once we have lists this should be a list.
        print "%d Favorites:" % len(ids)
        identifiers = self._gutenberg_id_lookup(ids)
        for identifier in identifiers:
            identifier.add_measurement(
                self.data_source, Measurement.GUTENBERG_FAVORITE,
                1)
            print "", self._title(identifier)

    def set_download_counts(self, all_download_counts):
        print "Downloads:"
        identifiers = self._gutenberg_id_lookup(all_download_counts.keys())
        for identifier in identifiers:
            identifier.add_measurement(
                self.data_source, Measurement.DOWNLOADS,
                all_download_counts[identifier.identifier])
            print "%d\t%s" % (
                all_download_counts[identifier.identifier],
                self._title(identifier))

    def classify(self, all_classifications):
        for classification, ids in all_classifications.items():
            identifiers = self._gutenberg_id_lookup(ids)
            for identifier in identifiers:
                identifier.classify(
                    self.data_source, Subject.GUTENBERG_BOOKSHELF, 
                    classification)
                print "%s\t%s" % (classification, self._title(identifier))

    def process_shelf(self, representation, handled):
        texts = set()
        favorites = set()
        downloads = dict()
        soup = BeautifulSoup(representation.content, "lxml")
        for book in soup.find_all("a", href=self.gutenberg_text_number):
            is_favorite = book.parent.find(
                'img', src=re.compile("Favorite-icon")) is not None
            m = self.gutenberg_text_number.search(book['href'])
            if m:
                identifier = m.groups()[0]
            else:
                set_trace()

            texts.add(identifier)
            if is_favorite:
                favorites.add(identifier)

        catalog_search_link = soup.find('a', text="catalog search", href=True)
        if catalog_search_link:
            url = catalog_search_link['href']
            new_texts, downloads = self.process_catalog_search(
                representation.url, url, handled)
            texts = texts.union(new_texts)

        return texts, favorites, downloads

    def process_catalog_search(self, referer, url, handled):
        texts = set()
        downloads = dict()
        while url:
            representation = self.do_get(referer, url, handled)
            if not representation:
                return texts, downloads
            soup = BeautifulSoup(representation.content, "lxml")
            for book in soup.find_all('li', 'booklink'):
                link = book.find('a', 'link', href=self.gutenberg_text_number)
                identifier = self.gutenberg_text_number.search(link['href']).groups()[0]
                texts.add(identifier)
                download_count_tag = book.find(
                    'span', 'extra', text=self.number_of_downloads)
                if download_count_tag:
                    download_count = self.number_of_downloads.search(
                        download_count_tag.text).groups()[0]
                    downloads[identifier] = int(download_count)

            next_link = soup.find('a', accesskey='+')
            if next_link:
                url = next_link['href']
            else:
                url = None
        return texts, downloads

    def process_bookshelf_list_page(self, representation):
        lists = []
        shelves = []
        soup = BeautifulSoup(representation.content, "lxml")
        # If this is a multi-page list, the next page counts as a list.
        next_link = soup.find("a", text="next 200", href=True)
        if next_link:
            lists.append(next_link['href'])
        for i in soup.find_all("a", href=re.compile("^/wiki/.*Bookshelf")):
            new_url = i['href']
            if '/wiki/Category:' in new_url:
                lists.append(new_url)
            elif new_url.endswith("Bookshelf)"):
                bookshelf_name = i.text
                if bookshelf_name.endswith("(Bookshelf)"):
                    bookshelf_name = bookshelf_name[:-len("(Bookshelf)")]
                bookshelf_name = bookshelf_name.strip()
                shelves.append((new_url, bookshelf_name))
        return lists, shelves


# class PopularityScraper(object):

#     start_url = "http://www.gutenberg.org/ebooks/search/?sort_order=downloads"

#     def scrape(self):
#         previous_page = None
#         next_page = self.start_url
#         while next_page:
#             previous_page, next_page = self.scrape_page(
#                 previous_page, next_page)
#             time.sleep(5 + random.random())

#     def scrape_page(self, referer, url):
#         headers = dict()
#         if referer:
#             headers['Referer']=referer
#         response = requests.get(url, headers=headers)
#         if response.status_code != 200:
#             raise Exception("Request to %s got status code %s: %s" % (
#                 url, response.status_code, response.content))
#         soup = BeautifulSoup(response.content, 'lxml')
#         set_trace()
#         for book in soup.find_all('li', 'booklink'):
#             id = book.find('a')['href']
#             downloads = book.find('span', 'extra')
#             print id, downloads

#         next_page = soup.find(accesskey='+')
#         if next_page:
#             return url, urljoin(url, next_page['href'])
#         else:
#             return None, None
            
