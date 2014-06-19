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
        self.parser = etree.HTMLParser()

    def request(self, url):
        return requests.request(url)

    def _extract_text_nodes(self, content):
        tree = etree.fromstring(content, self.parser)
        for i in tree.xpath("(descendant::text() | following::text())"):
            i = i.strip()
            if i:
                yield i.split('=', 1)

    def dump(self, barcode):
        url = urljoin(self.root, "/%(barcode)s/dump" % dict(barcode=barcode))
        response = self.request(url)
        return dict(self._extract_text_nodes(response.content))

    def pintest(self, barcode, pin):
        url = urljoin(
            self.root, "/%(barcode)s/%(pin)s/pintest" % dict(
                barcode=barcode, pin=pin))
        response = self.request(url)
        data = dict(self._extract_text_nodes(response.content))
        if data.get('RETCOD') == '0':
            return True
        return False

