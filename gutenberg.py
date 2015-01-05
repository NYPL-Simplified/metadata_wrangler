import datetime
import json
import os
import random
import re
import requests
import shutil
from StringIO import StringIO
import tarfile
import time
from urlparse import urljoin, urlparse

from bs4 import BeautifulSoup

from nose.tools import set_trace

from core.model import (
    get_one_or_create,
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

from core.monitor import Monitor
from oclc import (
    OCLCClassifyAPI,
    OCLCXMLParser,
)
from core.util import LanguageCodes

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
    
    def __init__(self, _db):
        self._db = _db
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
                swid_xml = self.oclc_classify.lookup_by(wi=swid)
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
                    raise IOError("Got unexpected representation type from lookup: %s" % representation_type)
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
    MOST_POPULAR_URL = 'http://www.gutenberg.org/ebooks/search/%3Fsort_order%3Ddownloads'
    gutenberg_text_number = re.compile("/ebooks/([0-9]+)")
    number_of_downloads = re.compile("([0-9]+) download")

    def __init__(self, _db):
        self._db = _db
        self.data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

    def do_get_with_captcha_trapdoor(self, *args, **kwargs):
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
        handled = set()
        ignore, all_download_counts = self.process_catalog_search(
            None, self.MOST_POPULAR_URL, handled)

        lists_of_shelves = [(None, self.BASE_URL)]
        shelves = []
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
        # Favorites turns out not to be that useful.
        # self.set_favorites(all_favorites)
        self.set_download_counts(all_download_counts)
        self.classify(all_classifications)

    def _title(self, identifier):
        a = identifier.primarily_identifies
        if not a:
            return "(unknown)"
        else:
            return a[0].title.encode("utf8")

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
                print "%s\t%s" % (classification.encode("utf8"), self._title(identifier))

    def process_shelf(self, representation, handled):
        texts = set()
        favorites = set()
        downloads = dict()
        soup = BeautifulSoup(representation.content, "lxml")
        for book in soup.find_all("a", href=self.gutenberg_text_number):
            is_favorite = book.parent.find(
                'img', src=re.compile("Favorite-icon")) is not None
            m = self.gutenberg_text_number.search(book['href'])
            identifier = m.groups()[0]

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
