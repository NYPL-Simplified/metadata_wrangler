from nose.tools import set_trace
from lxml import etree
from urlparse import urljoin
from urllib import urlencode

from integration import XMLParser
from config import CONFIG

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

class DummyMilleniumPatronAPI(object):

    user1 = { 'PATRN NAME[pn]' : "SHELDON, ALICE",
              'RECORD #[p81]' : "12345",
              'P BARCODE[pb]' : "0",
              '.pin' : '0000'}
    user2 = { 'PATRN NAME[pn]' : "HEINLEIN, BOB",
              'RECORD #[p81]' : "67890",
              'P BARCODE[pb]' : "5",
              '.pin' : '5555'}

    users = [user1, user2]

    def pintest(self, barcode, pin):
        "A valid test PIN is the first character of the barcode repeated four times."
        return pin == barcode[0] * 4

    def dump(self, barcode):
        for u in self.users:
            if user['P BARCODE[pb]'] == barcode:
                d = dict(u)
                del d['.pin']
                return d
        return dict(ERRNUM='1', ERRMSG="Requested record not found")


