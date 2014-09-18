import base64
import urlparse
import datetime
import time
import hmac
import hashlib
import os
import requests
from lxml import etree
import json

from nose.tools import set_trace
from model import (
    get_one_or_create,
    CirculationEvent,
    CoverageProvider,
    DataSource,
    LicensePool,
    WorkIdentifier,
    WorkRecord,
)

from integration import (
    XMLParser,
    FilesystemCache,
)
from monitor import Monitor

class ThreeMAPI(object):

    # TODO: %a and %b are localized per system, but 3M requires
    # English.
    AUTH_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
    ARGUMENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    AUTHORIZATION_FORMAT = "3MCLAUTH %s:%s"

    DATETIME_HEADER = "3mcl-Datetime"
    AUTHORIZATION_HEADER = "3mcl-Authorization"
    VERSION_HEADER = "3mcl-APIVersion"

    def __init__(self, data_dir, account_id=None, library_id=None, account_key=None,
                 base_url = "http://cloudlibraryapi.3m.com/",
                 version="1.0"):
        self.version = version
        self.library_id = library_id or os.environ['THREEM_LIBRARY_ID']
        self.account_id = account_id or os.environ['THREEM_ACCOUNT_ID']
        self.account_key = account_key or os.environ['THREEM_ACCOUNT_KEY']
        self.base_url = base_url
        self.event_cache = FilesystemCache(
            os.path.join(data_dir, "cache", "events"))
        self.bibliographic_cache = FilesystemCache(
            os.path.join(data_dir, "cache", "bibliographic"), 3)
        self.item_list_parser = ItemListParser()

    def now(self):
        """Return the current GMT time in the format 3M expects."""
        return time.strftime(self.AUTH_TIME_FORMAT, time.gmtime())

    def sign(self, method, headers, path):
        """Add appropriate headers to a request."""
        authorization, now = self.authorization(method, path)
        headers[self.DATETIME_HEADER] = now
        headers[self.VERSION_HEADER] = self.version
        headers[self.AUTHORIZATION_HEADER] = authorization

    def authorization(self, method, path):
        signature, now = self.signature(method, path)
        auth = self.AUTHORIZATION_FORMAT % (self.account_id, signature)
        return auth, now

    def signature(self, method, path):
        now = self.now()
        signature_components = [now, method, path]
        signature_string = "\n".join(signature_components)
        digest = hmac.new(self.account_key, msg=signature_string,
                    digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        return signature, now

    def request(self, path, body=None, method="GET", cache=None, cache_key=None):
        if cache and cache.exists(cache_key):
            print " Cached! %s" % cache_key
            return cache.open(cache_key).read()
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith("/cirrus"):
            path = "/cirrus/library/%s%s" % (self.library_id, path)
        url = urlparse.urljoin(self.base_url, path)
        headers = {}
        self.sign(method, headers, path)
        if cache:
            print " %s <= %s" % (cache_key, url)
        else:
            print url
        response = requests.request(method, url, data=body, headers=headers)
        data = response.text
        if cache:
            cache.store(cache_key, data)
        return data

    def get_patron_circulation(self, patron_id):
        path = "circulation/patron/%s" % patron_id
        return self.request(path)

    def place_hold(self, item_id, patron_id):
        path = "placehold"
        body = "<PlaceHoldRequest><ItemId>%s</ItemId><PatronId>%s</PatronId></PlaceHoldRequest>" % (item_id, patron_id)
        return self.request(path, body, method="PUT")

    def cancel_hold(self, item_id, patron_id):
        path = "cancelhold"
        body = "<CancelHoldRequest><ItemId>%s</ItemId><PatronId>%s</PatronId></CancelHoldRequest>" % (item_id, patron_id)
        return self.request(path, body, method="PUT")

    def get_events_between(self, start, end, cache=False):
        """Return event objects for events between the given times."""
        start = start.strftime(self.ARGUMENT_TIME_FORMAT)
        end = end.strftime(self.ARGUMENT_TIME_FORMAT)
        url = "data/cloudevents?startdate=%s&enddate=%s" % (start, end)
        if cache:
            cache = self.event_cache
            key = start + "-" + end
        else:
            cache = None
            key = None
        data = self.request(url, cache=cache, cache_key=key)
        events = EventParser().process_all(data)
        return events

    def get_circulation_for(self, items):
        """Return circulation objects for the selected items."""
        increment = 25
        start = 0
        stop = increment
        #chunk = [x[LicensedWork.SOURCE_ID] for x in items[start:stop]]
        while chunk:
            url = "/circulation/items/" + ",".join(map(str, chunk))
            data = self.request(url).text
            for circ in CirculationParser().process_all(data):
                yield circ
            start += increment
            stop += increment
            #chunk = [x[LicensedWork.SOURCE_ID] for x in items[start:stop]]

    def get_bibliographic_info_for(self, ids):
        results = dict()
        uncached_ids = set(ids)
        for id in ids:
            if self.bibliographic_cache.exists(id):
                results[id] = self.bibliographic_cache.open(id).read()
                uncached_ids.remove(id)

        url = "/items/" + ",".join(map(str, uncached_ids))
        response = self.request(url)
        for (id, raw, cooked) in self.item_list_parser.parse(response):
            self.bibliographic_cache.store(id, raw)
            results[id] = cooked
        return results
      
class CirculationParser(XMLParser):

    """Parse 3M's circulation XML dialect into LicensedWork dictionaries."""

    # Map our names to 3M's names.
    # NAMES = {
    #     LicensedWork.SOURCE_ID : "ItemId",
    #     LicensedWork.ISBN : "ISBN13",
    #     LicensedWork.OWNED : "TotalCopies",
    #     LicensedWork.AVAILABLE : "AvailableCopies", 
    #     LicensedWork.CHECKOUTS : "Checkouts", 
    #     LicensedWork.HOLDS : "Holds",
    #     LicensedWork.RESERVES : "Reserves",
    # }

    def process_all(self, string):
        for i in super(CirculationParser, self).process_all(
                string, "//ItemCirculation"):
            yield i

    def process_one(self, tag, namespaces):
        if not tag.xpath(self.NAMES[LicensedWork.SOURCE_ID]):
            # This happens for events associated with books
            # no longer in our collection.
            return None

        item = LicensedWork()

        # Grab strings
        for outkey in [LicensedWork.SOURCE_ID, LicensedWork.ISBN]:
            inkey = self.NAMES[outkey]
            value = self.text_of_subtag(tag, inkey)
            item[outkey] = value

        for outkey in LicensedWork.OWNED, LicensedWork.AVAILABLE:
            inkey = self.NAMES[outkey]
            value = self.int_of_subtag(tag, inkey)
            item[outkey] = value

        # Counts of patrons who have the book in a certain state.
        for outkey in [LicensedWork.CHECKOUTS, LicensedWork.HOLDS,
                  LicensedWork.RESERVES]:
            inkey = self.NAMES[outkey]
            t = tag.xpath(inkey)[0]
            value = int(t.xpath("count(Patron)"))
            item[outkey] = value

        return item


class ItemListParser(XMLParser):

    # BASIC_FIELDS = {
    #     Edition.SOURCE_ID : "ItemId",
    #     Edition.TITLE: "Title",
    #     Edition.SUBTITLE: "SubTitle",
    #     Edition.ISBN: "ISBN13",
    #     Edition.LANGUAGE : "Language",
    #     Edition.PUBLISHER: "Publisher",
    #     Edition.FILE_SIZE: "Size",
    #     Edition.NUMBER_OF_PAGES: "NumberOfPages",
    # }

    def parse(self, xml):
        for i in self.process_all(xml, "//Item"):
            yield i

    @classmethod
    def parse_author_string(cls, string):
        authors = []
        for author in string.split(";"):
            authors.append({Edition.NAME: author.strip()})
        return authors

    def process_one(self, tag, namespaces):
        item = dict()
        for outkey, inkey in self.BASIC_FIELDS.items():
            value = self.text_of_optional_subtag(tag, inkey)
            if value:
                item[outkey] = value

        author_string = self.text_of_optional_subtag(tag, 'Authors')
        item[Edition.AUTHOR] = self.parse_author_string(author_string)

        summary = self.text_of_optional_subtag(tag, "Description")
        if summary:
            item[Edition.SUMMARY] = { 
                Edition.TEXT_TYPE : "html",
                Edition.TEXT_VALUE : summary
            }
        published = self.text_of_optional_subtag(tag, "PubDate")
        if not published:
            published = self.text_of_optional_subtag(tag, "PubYear")
        if published:
            item[Edition.DATE_PUBLISHED] = published

        for inkey, relation in [
                ("CoverLinkURL", Edition.IMAGE),
                ("BookLinkURL", "alternate")]:
            link = self.text_of_optional_subtag(tag, inkey)
            if link:
                if not Edition.LINKS in item:
                    item[Edition.LINKS] = {}
                item[Edition.LINKS][relation] = [{Edition.LINK_TARGET:link}]
        # We need to return an (ID, raw, cooked) 3-tuple
        return (item[Edition.SOURCE_ID],
              etree.tostring(tag),
              item)


class EventParser(XMLParser):

    """Parse 3M's event file format into our native event objects."""

    EVENT_SOURCE = "3M"
    INPUT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    # Map 3M's event names to our names.
    EVENT_NAMES = {
        "CHECKOUT" : CirculationEvent.CHECKOUT,
        "CHECKIN" : CirculationEvent.CHECKIN,
        "HOLD" : CirculationEvent.HOLD_PLACE,
        "RESERVED" : CirculationEvent.AVAILABILITY_NOTIFY,
        "PURCHASE" : CirculationEvent.LICENSE_ADD,
        "REMOVED" : CirculationEvent.LICENSE_REMOVE,
    }

    def process_all(self, string):
        for i in super(EventParser, self).process_all(
                string, "//CloudLibraryEvent"):
            yield i

    def process_one(self, tag, namespaces):
        isbn = self.text_of_subtag(tag, "ISBN")
        threem_id = self.text_of_subtag(tag, "ItemId")
        patron_id = self.text_of_subtag(tag, "PatronId")

        start_time = self.text_of_subtag(tag, "EventStartDateTimeInUTC")
        start_time = datetime.datetime.strptime(
                start_time, self.INPUT_TIME_FORMAT)
        end_time = self.text_of_subtag(tag, "EventEndDateTimeInUTC")
        end_time = datetime.datetime.strptime(
            end_time, self.INPUT_TIME_FORMAT)

        threem_event_type = self.text_of_subtag(tag, "EventType")
        internal_event_type = self.EVENT_NAMES[threem_event_type]

        return (threem_id, isbn, patron_id, start_time, end_time,
                internal_event_type)


class ThreeMCirculationMonitor(Monitor):

    """Maintain license pool for 3M titles.

    This is where new books are given their LicensePools.  But the
    bibliographic data isn't inserted into those LicensePools until
    the ThreeMBibliographicMonitor runs.
    """

    def __init__(self, data_directory, default_start_time=None,
                 account_id=None, library_id=None, account_key=None):
        super(ThreeMCirculationMonitor, self).__init__(
            "3M Circulation Monitor", default_start_time=default_start_time)
        path = os.path.join(data_directory, DataSource.THREEM)
        if not os.path.exists(path):
            os.makedirs(path)
        self.source = ThreeMAPI(path, account_id, library_id, account_key)

    def slice_timespan(self, start, cutoff, increment):
        slice_start = start
        while slice_start < cutoff:
            full_slice = True
            slice_cutoff = slice_start + increment
            if slice_cutoff > cutoff:
                slice_cutoff = cutoff
                full_slice = False
            yield slice_start, slice_cutoff, full_slice
            slice_start = slice_start + increment

    def run_once(self, _db, start, cutoff):
        added_books = 0
        threem_data_source = DataSource.lookup(_db, DataSource.THREEM)

        i = 0
        one_day = datetime.timedelta(days=1)
        for start, cutoff, full_slice in self.slice_timespan(
                start, cutoff, one_day):
            events = self.source.get_events_between(start, cutoff, full_slice)
            for event in events:
                self.handle_event(_db, threem_data_source, *event)
                i += 1
                if not i % 1000:
                    print i
                    _db.commit()
            self.timestamp.timestamp = cutoff
        print "Handled %d events total" % i

    def handle_event(self, _db, data_source, threem_id, isbn, foreign_patron_id,
                     start_time, end_time, internal_event_type):
        # Find or lookup the LicensePool for this event.
        license_pool, is_new = LicensePool.for_foreign_id(
            _db, data_source, WorkIdentifier.THREEM_ID, threem_id)
        threem_identifier = license_pool.identifier
        isbn, ignore = WorkIdentifier.for_foreign_id(
            _db, WorkIdentifier.ISBN, isbn)

        # Create an empty WorkRecord for this LicensePool.
        work_record, ignore = WorkRecord.for_foreign_id(
            _db, data_source, WorkIdentifier.THREEM_ID, threem_id)

        # The ISBN and the 3M identifier are exactly equivalent.
        threem_identifier.equivalent_to(data_source, isbn, strength=1)

        # Log the event.
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=internal_event_type, start=start_time,
            foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(delta=1,end=end_time)
            )

        # If this is our first time seeing this LicensePool, log its
        # occurance as a separate event
        if is_new:
            event = get_one_or_create(
                _db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=license_pool.last_checked,
                    delta=1,
                    end=license_pool.last_checked,
                )
            )


class ThreeMBibliographicMonitor(CoverageProvider):
    """Fill in bibliographic metadata for 3M records."""

    def __init__(self, _db, data_directory,
                 account_id=None, library_id=None, account_key=None):
        path = os.path.join(data_directory, DataSource.THREEM)
        self.source = ThreeMAPI(path, account_id, library_id, account_key)
        self._db = _db
        self.input_source = DataSource.lookup(_db, DataSource.THREEM)
        self.output_source = DataSource.lookup(_db, DataSource.THREEM)
        super(ThreeMBibliographicMonitor, self).__init__(
            "3M Bibliographic Monitor",
            self.input_source, self.output_source)
        self.current_batch = []

    def process_work_record(self, wr):
        self.current_batch.append(wr)
        if len(self.current_batch) == 25:
            self.process_batch(self.current_batch)
            self.current_batch = []

    def process_batch(self, batch):
        identifiers = [x.primary_identifier.identifier for x in batch]
        for info in self.source.get_bibliographic_info_for(identifiers):
            self.annotate_work_record_with_bibliographic_information(
                self._db, wr, info, self.input_source
            )

# class ThreeMMetadataMonitor(MetadataMonitor):

#     DEFAULT_BATCH_SIZE = 25
#     URL_TEMPLATE = "/items/"

#     def __init__(self, _db, data_directory):
#         self.overdrive = OverdriveAPI(data_directory)
#         self._db = _db
#         self.input_source = DataSource.lookup(_db, DataSource.THREEM)
#         self.output_source = DataSource.lookup(_db, DataSource.THREEM)
#         self.source = ThreeMAPI(
#             _creds.account_id, _creds.library_id, _creds.account_key)
#         super(ThreeMCirculationMonitor, self).__init__(
#             "3M Circulation Monitor", self.input_source, self.output_source)

#     def __init__(self, data_directory, batch_size=None):
#         source = ThreeMAPI(
#             _creds.account_id, _creds.library_id, _creds.account_key)
#         circulation = ThreeMCirculationMonitor(data_directory).store
#         metadata = FilesystemMetadataStore(circulation.data_directory)
#         self.parser = ItemListParser()
#         super(ThreeMMetadataMonitor, self).__init__(
#             source, circulation, metadata, batch_size)

#     def retrieve_metadata_batch(self, ids):
#         url = self.URL_TEMPLATE + ",".join(map(str, ids))
#         response = self.source.request(url)
#         raw = response.text
#         for (id, raw, cooked) in self.parser.parse(response.text):
#             self.metadata.store(id, raw, cooked)

