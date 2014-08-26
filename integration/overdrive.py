import base64
import datetime
import os
import json
import requests
import time
import urlparse
import urllib
import logging

from model import Event, LicensedWork

from integration.circulation import (
    CirculationMonitor,
    FilesystemMonitorStore,
)

import _overdrive_creds as _creds

class OverdriveAPI(object):

    CRED_FILE = "oauth_cred.json"
    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    # The ebook formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"

    EVENTS_URL = "http://api.overdrive.com/v1/collections/%(collection)s/products?lastupdatetime=%(lastupdatetime)s&sort=%(sort)s&formats=%(formats)s&limit=%(limit)s"

    def __init__(self, data_directory):
        self.credential_path = os.path.join(data_directory, self.CRED_FILE)
        self.check_creds()

    def check_creds(self):
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
        URL = "https://oauth.overdrive.com/token"
        auth = "%s:%s" % (_creds.CLIENT_KEY, _creds.CLIENT_SECRET)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        headers['Authorization'] = "Basic %s" % base64.b64encode(auth)
        print "Refreshing OAuth credential."
        payload = dict(grant_type="client_credentials")
        response = requests.post(URL, payload, headers=headers)
        open(self.credential_path, "w").write(response.text)

    def get_patron_token(self, barcode, pin):
        data = dict(username=barcode, password=pin,
                    grant_type="password",
                    scope="websiteid:???",
                    authorizationname="default")

        

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

    def get(self, url):
        headers = dict(Authorization="Bearer %s" % self.token)
        return requests.get(url, headers=headers)

    def get_library(self):
        url = "http://api.overdrive.com/v1/libraries/%s" % _creds.LIBRARY_ID
        return self.get(url)

    def get_events_between(self, start, cutoff, circulation_to_refresh):
        """Get events between the start time and now."""
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        params = dict(lastupdatetime=start,
                      formats=self.FORMATS,
                      sort="popularity:desc",
                      limit=self.PAGE_SIZE_LIMIT,
                      collection=_creds.COLLECTION_NAME)
        next_link = self.make_link_safe(self.EVENTS_URL % params)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            for i in page_inventory:
                print "Adding %r to the list." % i
                circulation_to_refresh.append(i)

        # We don't know about any events yet. All events for this
        # monitor are generated when we check inventory.
        return []

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
            OverdriveRepresentationExtractor.availability_info(data))
        return availability_queue, next_link

    def get_circulation_for(self, to_check):
        """Check the circulation status for a bunch of books."""
        for i, data in enumerate(to_check):
            if i % 50 == 0 and i != 0:
                print "%s/%s" % (i, len(to_check))
            now = datetime.datetime.utcnow()

            # Retrieve current circulation information about this book
            circulation_link = data['availability_link']
            response = self.get(circulation_link)
            if response.status_code != 200:
                print "ERROR: Could not get availability for %s: %s" % (id, 
response.status_code)
                return

            # Build a new circulation dictionary
            circulation = OverdriveRepresentationExtractor.circulation_info(
                response.json())
            circulation[LicensedWork.TITLE] = data[LicensedWork.TITLE]

            # When we yield this item, its data will be compared with
            # local inventory data, and events will be generated
            # corresponding to the differences.
            yield circulation

class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    @classmethod
    def availability_info(self, book_list):
        """Yields a dictionary with a link to availability info for each book."""
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            data = dict()
            for inkey, outkey in (
                    ('id', LicensedWork.SOURCE_ID),
                    ('title', LicensedWork.TITLE)):
                data[outkey] =  product[inkey]
            links = product['links']
            if 'availability' in links:
                data['availability_link'] = links['availability']['href']
            else:
                log.warn("No availability link for %s" % book_id)
            l.append(data)
        return l

    @classmethod
    def circulation_info(self, book):
        data = dict()
        for (inkey, outkey) in [
                ('id', LicensedWork.SOURCE_ID),
                ('copiesOwned', LicensedWork.OWNED),
                ('copiesAvailable', LicensedWork.AVAILABLE),
                ('numberOfHolds', LicensedWork.HOLDS)]:
            data[outkey] = book[inkey]
            data[LicensedWork.RESERVES] = 0
            data[LicensedWork.CHECKOUTS] = (
                book['copiesOwned'] - book['copiesAvailable'])
        return data

    @classmethod
    def link(self, page, rel):
        if rel in page['links']:
            raw_link = page['links'][rel]['href']
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link


class OverdriveCirculationMonitor(CirculationMonitor):
    
    # How stale an inventory record is allowed to get.
    #
    # Overdrive doesn't tell us about inventory events directly. We
    # set this to None to tell CirculationMonitor to check every time.
    MAXIMUM_STALE_TIME = None

    def __init__(self, data_directory):
        path = os.path.join(data_directory, OverdriveAPI.EVENT_SOURCE)
        source = OverdriveAPI(path)
        store = FilesystemMonitorStore(path)
        super(OverdriveCirculationMonitor, self).__init__(
            source, store, 60, self.MAXIMUM_STALE_TIME)
