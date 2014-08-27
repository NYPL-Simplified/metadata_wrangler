import base64
import datetime
import os
import json
import requests
import time
import urlparse
import urllib
import logging
from nose.tools import set_trace

from model import (
    get_one_or_create,
    CirculationEvent,
    DataSource,
    LicensePool,
    WorkIdentifier,
    WorkRecord,
)

from integration import (
    FilesystemCache
)
from monitor import Monitor

class OverdriveAPI(object):

    TOKEN_ENDPOINT = "https://oauth.overdrive.com/token"
    PATRON_TOKEN_ENDPOINT = "https://oauth-patron.overdrive.com/patrontoken"

    LIBRARY_ENDPOINT = "http://api.overdrive.com/v1/libraries/%(library_id)s"
    METADATA_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "http://api.overdrive.com/v1/collections/%(collection_name)s/products?lastupdatetime=%(lastupdatetime)s&sort=%(sort)s&formats=%(formats)s&limit=%(limit)s"

    CRED_FILE = "oauth_cred.json"
    BIBLIOGRAPHIC_DIRECTORY = "bibliographic"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    # The ebook formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"

    
    def __init__(self, data_directory):

        self.bibliographic_cache = FilesystemCache(
            os.path.join(data_directory, self.BIBLIOGRAPHIC_DIRECTORY),
            subdir_chars=4)
        self.credential_path = os.path.join(data_directory, self.CRED_FILE)

        # Set some stuff from environment variables
        self.client_key = os.environ['OVERDRIVE_CLIENT_KEY']
        self.client_secret = os.environ['OVERDRIVE_CLIENT_SECRET']
        self.website_id = os.environ['OVERDRIVE_WEBSITE_ID']
        self.library_id = os.environ['OVERDRIVE_LIBRARY_ID']
        self.collection_name = os.environ['OVERDRIVE_COLLECTION_NAME']

        # Get set up with up-to-date credentials from the API.
        self.check_creds()
        self.collection_token = self.get_library()['collectionToken']

    def check_creds(self):
        """If the Bearer Token is about to expire, update it."""
        refresh = True
        if os.path.exists(self.credential_path):
            cred_mod_time = os.stat(self.credential_path).st_mtime
            cred_age = time.time() - cred_mod_time
            if cred_age <= self.MAX_CREDENTIAL_AGE:
                refresh = False
        if refresh:
            self.refresh_creds()
            print "Refreshed OAuth credential."
        self.token = json.load(open(self.credential_path))['access_token']

    def refresh_creds(self):
        """Fetch a new Bearer Token and write it to disk."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"))
        open(self.credential_path, "w").write(response.text)

    def get(self, url):
        """Make an HTTP GET request using the active Bearer Token."""
        headers = dict(Authorization="Bearer %s" % self.token)
        return requests.get(url, headers=headers)

    def token_post(self, url, payload, headers={}):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.encodestring(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        return requests.post(url, payload, headers=headers)

    def get_patron_token(self, barcode, pin):
        """Create an OAuth token for the given patron."""
        payload = dict(
            grant_type="password",
            username=library_card,
            password=pin,
            scope="websiteid:%s authorizationname:%s" % (
                self.website_id, "default")
        )
        response = self.token_post(patron_token_endpoint, payload)
        return response.content

    def get_library(self):
        url = self.LIBRARY_ENDPOINT % dict(library_id=self.library_id)
        return self.get(url).json()

    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        books = []
        params = dict(lastupdatetime=start,
                      formats=self.FORMATS,
                      sort="popularity:desc",
                      limit=self.PAGE_SIZE_LIMIT,
                      collection_name=self.collection_name)
        next_link = self.make_link_safe(self.EVENTS_ENDPOINT % params)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            for i in page_inventory:
                print "Adding %r to the list." % i
                books.append(i)

        return books

    def metadata_lookup(self, overdrive_id):
        """Look up metadata for an Overdrive ID.

        Update the corresponding WorkRecord appropriately.
        """
        cache_key = overdrive_id + ".json"
        if self.cache.exists(cache_key):
            raw = self.cache.open(cache_key).read()
        else:
            url = self.METADATA_ENDPOINT % dict(
                collection_token=self.collection_token,
                item_id=overdrive_id
                )
            print "%s => %s" % (url, self.cache._filename(cache_key))
            response = self.get(url)
            if response.status_code != 200:
                raise IOError(response.status_code)
            self.cache.store(cache_key, response.content)

            set_trace()

    def update_licensepool(self, _db, data_source, book):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        # Retrieve current circulation information about this book
        circulation_link = book['availability_link']
        response = self.get(circulation_link)
        if response.status_code != 200:
            print "ERROR: Could not get availability for %s: %s" % (id, 
response.status_code)
            return

        book.update(response.json())
        return self._update_licensepool(_db, data_source, book)

    def _update_licensepool(self, _db, data_source, book):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Also adds very basic bibliographic information to the WorkRecord.
        """
        overdrive_id = book['id']
        pool, was_new = LicensePool.for_foreign_id(
            _db, data_source, WorkIdentifier.OVERDRIVE_ID, overdrive_id)
        if was_new:
            pool.open_access = False
            wr, wr_new = WorkRecord.for_foreign_id(
                _db, data_source, WorkIdentifier.OVERDRIVE_ID, overdrive_id)
            wr.title = book['title']
            if 'author_name' in book:
                name = book['author_name']
                if name:
                    contributor = wr.add_contributor(name, 'Author')
                    contributor.display_name = name
            print "New book: %r" % wr

        pool.licenses_owned = book['copiesOwned']
        pool.licenses_available = book['copiesAvailable']
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = book['numberOfHolds']
        pool.last_checked = datetime.datetime.utcnow()
        return pool, was_new

    def _get_book_list_page(self, link):
        """Process a page of inventory whose circulation we need to check.

        Returns a list of (title, id, availability_link) 3-tuples,
        plus a link to the next page of results.
        """
        response = self.get(link)
        try:
            data = response.json()
        except Exception, e:
            print "ERROR: %s" % response
            set_trace()
            return [], None

        # Find the link to the next page of results, if any.
        next_link = OverdriveRepresentationExtractor.link(data, 'next')

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = (
            OverdriveRepresentationExtractor.availability_link_list(data))
        return availability_queue, next_link

    @classmethod
    def make_link_safe(self, url):
        """Turn a server-provided link into a link the server will accept!

        This is completely obnoxious and I have complained about it to
        Overdrive.
        """
        parts = list(urlparse.urlsplit(url))
        parts[2] = urllib.quote(parts[2])
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        parts[3] = query_string
        return urlparse.urlunsplit(tuple(parts))
            

class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    @classmethod
    def availability_link_list(self, book_list):
        """:return: A list of dictionaries with keys `id`, `title`,
        `contributor_names`, `availability_link`.
        """
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            data = dict(id=product['id'],
                        title=product['title'],
                        author_name=None)
            
            if 'primaryCreator' in product:
                creator = product['primaryCreator']
                if creator.get('role') == 'Author':
                    data['author_name'] = creator.get('name')
            links = product['links']
            if 'availability' in links:
                link = links['availability']['href']
                data['availability_link'] = OverdriveAPI.make_link_safe(link)
            else:
                log.warn("No availability link for %s" % book_id)
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if rel in page['links']:
            raw_link = page['links'][rel]['href']
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link



class OverdriveCirculationMonitor(Monitor):
    """Maintain license pool for Overdrive titles.

    This is where new books are given their LicensePools.  But the
    bibliographic data isn't inserted into those LicensePools until
    the OverdriveCoverageProvider runs.
    """
    def __init__(self, data_directory):
        super(OverdriveCirculationMonitor, self).__init__(
            "Overdrive Circulation Monitor")
        path = os.path.join(data_directory, DataSource.OVERDRIVE)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = OverdriveAPI(path)

    def run_once(self, _db, start, cutoff):
        added_books = 0
        books = self.source.recently_changed_ids(start, cutoff)
        overdrive_data_source = DataSource.lookup(
            _db, DataSource.OVERDRIVE)

        for i, book in enumerate(books):
            if i > 0 and not i % 50:
                print "%s/%s" % (i, len(to_check))
            license_pool, is_new = self.source.update_licensepool(
                _db, overdrive_data_source, book)
            # Log a circulation event for this work.
            if is_new:
                event = get_one_or_create(
                    _db, CirculationEvent,
                    type=CirculationEvent.TITLE_ADD,
                    license_pool=license_pool,
                    create_method_kwargs=dict(
                        start=license_pool.last_checked
                    )
                )
            _db.commit()
