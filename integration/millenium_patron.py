from nose.tools import set_trace
from lxml import etree
from urlparse import urljoin
from urllib import urlencode

from integration import XMLParser
from database_credentials import CONFIG

class MilleniumPatronAPI(XMLParser):

    def __init__(self):
        root = CONFIG['millenium']['root']
        self.root = root

    def request(self, url):
        return requests.request(url)

    def dump(self, barcode):
        url = urljoin(self.root, "/%(barcode)s/dump" % dict(barcode=barcode))
        response = self.request(url)
        set_trace()

    def pintest(self, barcode, pin):
        url = urljoin(
            self.root, "/%(barcode)s/%(pin)s/pintest" % dict(
                barcode=barcode, pin=pin))
        response = self.request(url)
        set_trace()
