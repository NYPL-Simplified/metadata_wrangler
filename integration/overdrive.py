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
    CoverageProvider,
    DataSource,
    LicensePool,
    Resource,
    Subject,
    WorkIdentifier,
    WorkRecord,
)

from integration import (
    FilesystemCache
)
from monitor import Monitor
from util import LanguageCodes

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
            os.path.join(data_directory, self.EVENT_SOURCE, 
                         self.BIBLIOGRAPHIC_DIRECTORY),
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
                print i
                yield i

    def metadata_lookup(self, overdrive_id):
        """Look up metadata for an Overdrive ID.

        Update the corresponding WorkRecord appropriately.
        """
        cache_key = overdrive_id + ".json"
        if self.bibliographic_cache.exists(cache_key):
            raw = self.bibliographic_cache.open(cache_key).read()
            return json.loads(raw)
        else:
            url = self.METADATA_ENDPOINT % dict(
                collection_token=self.collection_token,
                item_id=overdrive_id
                )
            print "%s => %s" % (url, self.bibliographic_cache._filename(cache_key))
            response = self.get(url)
            if response.status_code != 200:
                raise IOError(response.status_code)
            self.bibliographic_cache.store(cache_key, response.content)
            return response.json()

    def update_licensepool(self, _db, data_source, book, exception_on_401=False):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information.
        """
        # Retrieve current circulation information about this book
        circulation_link = book['availability_link']
        response = self.get(circulation_link)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                self.check_creds()
                return self.update_licensepool(_db, data_source, book, True)
        if response.status_code != 200:
            print "ERROR: Could not get availability for %s: %s" % (
                book['id'], response.status_code)
            return

        book.update(response.json())
        return self.update_licensepool_with_book_info(_db, data_source, book)

    @classmethod
    def update_licensepool_with_book_info(cls, _db, data_source, book):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Also adds very basic bibliographic information to the WorkRecord.
        """
        if data_source.name != DataSource.OVERDRIVE:
            raise ValueError(
                "You're supposed to pass in the Overdrive DataSource object so I can avoid looking it up. That's the only valid value for data_source.")
        overdrive_id = book['id']
        pool, was_new = LicensePool.for_foreign_id(
            _db, data_source, WorkIdentifier.OVERDRIVE_ID, overdrive_id)
        if was_new:
            pool.open_access = False
            wr, wr_new = WorkRecord.for_foreign_id(
                _db, data_source, WorkIdentifier.OVERDRIVE_ID, overdrive_id)
            if 'title' in book:
                wr.title = book['title']
            print "New book: %r" % wr

        pool.update_availability(
            book['copiesOwned'], book['copiesAvailable'], 0, 
            book['numberOfHolds'])
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
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlparse.urlunsplit(tuple(parts))
            

class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    @classmethod
    def availability_link_list(self, book_list):
        """:return: A list of dictionaries with keys `id`, `title`,
        `availability_link`.
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
            links = product.get('links', [])
            if 'availability' in links:
                link = links['availability']['href']
                data['availability_link'] = OverdriveAPI.make_link_safe(link)
            else:
                log.warn("No availability link for %s" % book_id)
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if 'links' in page and rel in page['links']:
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
        overdrive_data_source = DataSource.lookup(
            _db, DataSource.OVERDRIVE)

        for i, book in enumerate(self.source.recently_changed_ids(start, cutoff)):
            if i > 0 and not i % 50:
                print " %s processed" % i
            if not book:
                continue
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
        print "Processed %d books total." % i

class OverdriveBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for Overdrive records."""

    def __init__(self, _db, data_directory):
        self.overdrive = OverdriveAPI(data_directory)
        self._db = _db
        self.input_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        self.output_source = DataSource.lookup(_db, DataSource.OVERDRIVE)
        super(OverdriveBibliographicMonitor, self).__init__(
            "Overdrive Bibliographic Monitor",
            self.input_source, self.output_source)

    @classmethod
    def _add_value_as_resource(cls, input_source, identifier, pool, rel, value,
                               media_type="text/plain", url=None):
        if isinstance(value, str):
            value = value.decode("utf8")
        elif isinstance(value, unicode):
            pass
        else:
            value = str(value)
        identifier.add_resource(
            rel, url, input_source, pool, media_type, value)

    DATE_FORMAT = "%Y-%m-%d"

    def process_work_record(self, wr):
        identifier = wr.primary_identifier
        info = self.overdrive.metadata_lookup(identifier.identifier)
        return self.annotate_work_record_with_bibliographic_information(
            self._db, wr, info, self.input_source
        )

    media_type_for_overdrive_type = {
        "ebook-pdf-adobe" : "application/pdf",
        "ebook-pdf-open" : "application/pdf",
        "ebook-epub-adobe" : "application/epub+zip",
        "ebook-epub-open" : "application/epub+zip",
    }
        
    @classmethod
    def annotate_work_record_with_bibliographic_information(
            cls, _db, wr, info, input_source):

        identifier = wr.primary_identifier
        license_pool = wr.license_pool

        # First get the easy stuff.
        wr.title = info['title']
        wr.subtitle = info.get('subtitle', None)
        wr.series = info.get('series', None)
        wr.publisher = info.get('publisher', None)
        wr.imprint = info.get('imprint', None)

        if 'publishDate' in info:
            wr.published = datetime.datetime.strptime(
                info['publishDate'][:10], cls.DATE_FORMAT)

        languages = [
            LanguageCodes.two_to_three.get(l['code'], l['code'])
            for l in info.get('languages', [])
        ]
        if 'eng' in languages or not languages:
            wr.language = 'eng'
        else:
            wr.language = sorted(languages)[0]

        # TODO: Is there a Gutenberg book with this title and the same
        # author names? If so, they're the same. Merge the work and
        # reuse the Contributor objects.
        #
        # Or, later might be the time to do that stuff.

        for creator in info.get('creators', []):
            name = creator['fileAs']
            display_name = creator['name']
            role = creator['role']
            contributor = wr.add_contributor(name, role)
            contributor.display_name = display_name
            if 'bioText' in creator:
                contributor.extra = dict(description=creator['bioText'])

        for i in info.get('subjects', []):
            c = identifier.classify(input_source, Subject.OVERDRIVE, i['value'])

        extra = dict()
        for inkey, outkey in (
                ('gradeLevels', 'grade_levels'),
                ('mediaType', 'medium'),
                ('sortTitle', 'sort_title'),
                ('awards', 'awards'),
        ):
            if inkey in info:
                extra[outkey] = info.get(inkey)
        wr.extra = extra

        # Associate the Overdrive WorkRecord with other identifiers
        # such as ISBN.
        for format in info.get('formats', []):
            for new_id in format.get('identifiers', []):
                t = new_id['type']
                v = new_id['value']
                type_key = None
                if t == 'ASIN':
                    type_key = WorkIdentifier.ASIN
                elif t == 'ISBN':
                    type_key = WorkIdentifier.ISBN
                elif t == 'DOI':
                    type_key = WorkIdentifier.DOI
                elif t == 'UPC':
                    type_key = WorkIdentifier.UPC
                elif t == 'PublisherCatalogNumber':
                    continue
                if type_key:
                    new_identifier, ignore = WorkIdentifier.for_foreign_id(
                        _db, type_key, v)
                    identifier.equivalent_to(
                        input_source, new_identifier, 1)

            # Samples become resources.
            if 'samples' in format:
                if format['id'] == 'ebook-overdrive':
                    # Useless to us.
                    continue
                media_type = cls.media_type_for_overdrive_type.get(
                    format['id'])
                if not media_type:
                    print format['id']
                    set_trace()
                for sample_info in format['samples']:
                    href = sample_info['url']
                    resource, new = identifier.add_resource(
                        Resource.SAMPLE, href, input_source,
                        license_pool, media_type)
                    resource.file_size = format['fileSize']

        # Add resources: cover, descriptions, rating and popularity
        if info['starRating']:
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.RATING,
                info['starRating'])

        if info['popularity']:
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.POPULARITY,
                info['popularity'])

        if 'images' in info and 'cover' in info['images']:
            link = info['images']['cover']
            href = OverdriveAPI.make_link_safe(link['href'])
            media_type = link['type']
            identifier.add_resource(Resource.IMAGE, href, input_source,
                                    license_pool, media_type)

        short = info.get('shortDescription')
        full = info.get('fullDescription')

        if full:
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.DESCRIPTION, full,
                "text/html", "tag:full")

        if short and short != full:
            cls._add_value_as_resource(
                input_source, identifier, license_pool, Resource.DESCRIPTION, short,
                "text/html", "tag:short")

        return True
