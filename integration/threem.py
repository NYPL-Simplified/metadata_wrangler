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
    Contributor,
    CirculationEvent,
    CoverageProvider,
    DataSource,
    LicensePool,
    Resource,
    WorkIdentifier,
    WorkRecord,
)

from integration import (
    XMLParser,
    FilesystemCache,
)
from monitor import Monitor
from util import LanguageCodes

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

    def get_bibliographic_info_for(self, work_records):
        results = dict()
        identifiers = []
        wr_for_identifier = dict()
        for wr in work_records:
            identifier = wr.primary_identifier.identifier
            identifiers.append(identifier)
            wr_for_identifier[identifier] = wr
        uncached = set(identifiers)
        for identifier in identifiers:
            if self.bibliographic_cache.exists(identifier):
                wr = wr_for_identifier[identifier]
                data = self.bibliographic_cache.open(identifier).read()
                identifier, raw, cooked = list(self.item_list_parser.parse(data))[0]
                results[identifier] = (wr, cooked)
                uncached.remove(identifier)

        if uncached:
            url = "/items/" + ",".join(uncached)
            response = self.request(url)
            for (identifier, raw, cooked) in self.item_list_parser.parse(response):
                self.bibliographic_cache.store(identifier, raw)
                wr = wr_for_identifier[identifier]
                results[identifier] = (wr, cooked)
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

    DATE_FORMAT = "%Y-%m-%d"
    YEAR_FORMAT = "%Y"

    def parse(self, xml):
        for i in self.process_all(xml, "//Item"):
            yield i

    @classmethod
    def author_names_from_string(cls, string):
        if not string:
            return
        for author in string.split(";"):
            yield author.strip()

    def process_one(self, tag, namespaces):
        def value(threem_key):
            return self.text_of_optional_subtag(tag, threem_key)
        resources = dict()
        identifiers = dict()
        item = { Resource : resources,  WorkIdentifier: identifiers }

        identifiers[WorkIdentifier.THREEM_ID] = value("ItemId")
        identifiers[WorkIdentifier.ISBN] = value("ISBN13")

        item[WorkRecord.title] = value("Title")
        item[WorkRecord.subtitle] = value("SubTitle")
        item[WorkRecord.publisher] = value("Publisher")
        language = value("Language")
        language = LanguageCodes.two_to_three.get(language, language)
        item[WorkRecord.language] = language

        author_string = value('Authors')
        item[Contributor] = list(self.author_names_from_string(author_string))

        published_date = None
        published = value("PubDate")
        formats = [self.DATE_FORMAT, self.YEAR_FORMAT]
        if not published:
            published = value("PubYear")
            formats = [self.YEAR_FORMAT]

        for format in formats:
            try:
                published_date = datetime.datetime.strptime(published, format)
            except ValueError, e:
                pass

        if not published_date:
            set_trace()
        item[WorkRecord.published] = published_date

        resources[Resource.DESCRIPTION] = value("Description")
        resources[Resource.IMAGE] = value("CoverLinkURL").replace("&amp;", "&")
        resources["alternate"] = value("BookLinkURL").replace("&amp;", "&")
        return identifiers[WorkIdentifier.THREEM_ID], etree.tostring(tag), item


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


class ThreeMEventMonitor(Monitor):

    """Maintain license pool for 3M titles.

    This is where new books are given their LicensePools.  But the
    bibliographic data isn't inserted into those LicensePools until
    the ThreeMBibliographicMonitor runs.
    """

    def __init__(self, data_directory, default_start_time=None,
                 account_id=None, library_id=None, account_key=None):
        super(ThreeMCirculationMonitor, self).__init__(
            "3M Event Monitor", default_start_time=default_start_time)
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
        return True

    def commit_workset(self):
        # Process any uncompleted batch.
        self.process_batch(self.current_batch)
        super(ThreeMBibliographicMonitor, self).commit_workset()

    def process_batch(self, batch):
        for wr, info in self.source.get_bibliographic_info_for(
                batch).values():
            self.annotate_work_record_with_bibliographic_information(
                self._db, wr, info, self.input_source
            )
            print wr

    def annotate_work_record_with_bibliographic_information(
            self, db, wr, info, input_source):

        # ISBN and 3M ID were associated with the work record earlier,
        # so don't bother doing it again.

        pool = wr.license_pool
        identifier = wr.primary_identifier

        if not isinstance(info, dict):
            set_trace()

        wr.title = info[WorkRecord.title]
        wr.subtitle = info[WorkRecord.subtitle]
        wr.publisher = info[WorkRecord.publisher]
        wr.language = info[WorkRecord.language]
        wr.published = info[WorkRecord.published]

        for name in info[Contributor]:
            wr.add_contributor(name, Contributor.AUTHOR_ROLE)

        # Associate resources with the work record.
        for rel, value in info[Resource].items():
            if rel == Resource.DESCRIPTION:
                href = None
                media_type = "text/html"
                content = value
            else:
                href = value
                media_type = None
                content = None
            identifier.add_resource(rel, href, input_source, pool, media_type, content)

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

