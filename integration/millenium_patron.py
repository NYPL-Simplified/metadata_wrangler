from nose.tools import set_trace
from lxml import etree
from urlparse import urljoin
from urllib import urlencode
import datetime

from integration import XMLParser
import os

class MilleniumPatronAPI(XMLParser):

    EXPIRATION_FIELD = 'EXP DATE[p43]'
    EXPIRATION_DATE_FORMAT = '%m-%d-%y'

    def __init__(self):
        root = os.environ['MILLENIUM_HOST']
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

    @classmethod
    def active(self, dump):
        """Is this patron account active, or has it expired?"""
        # TODO: This opens up all sorts of questions about which time
        # zone 'today' is measured from. For now, I will simply use
        # the time zone of the server. (This is why I don't use
        # utcnow() here even though I use it everywhere else.)
        expires = dump.get(self.EXPIRATION_FIELD, None)
        if not expires:
            return False
        expires = datetime.datetime.strptime(
            expires, self.EXPIRATION_DATE_FORMAT).date()
        today = datetime.datetime.now().date()
        if expires >= today:
            return True
        return False


class DummyMilleniumPatronAPI(MilleniumPatronAPI):


    # This user's card has expired.
    user1 = { 'PATRN NAME[pn]' : "SHELDON, ALICE",
              'RECORD #[p81]' : "12345",
              'P BARCODE[pb]' : "0",
              'EXP DATE[p43]' : "04-01-05"
    }
    
    # This user's card still has ten days on it.
    the_future = datetime.datetime.utcnow() + datetime.timedelta(days=10)
    user2 = { 'PATRN NAME[pn]' : "HEINLEIN, BOB",
              'RECORD #[p81]' : "67890",
              'P BARCODE[pb]' : "5",
              'EXP DATE[p43]' : the_future.strftime("%m-%d-%y")
    }

    users = [user1, user2]

    def pintest(self, barcode, pin):
        "A valid test PIN is the first character of the barcode repeated four times."
        u = self.dump(barcode)
        if 'ERRNUM' in u:
            return False
        return pin == barcode[0] * 4

    def dump(self, barcode):
        # We have a couple custom barcodes.
        for u in self.users:
            if u['P BARCODE[pb]'] == barcode:
                return u
                
        # A barcode that starts with '404' does not exist.
        if barcode.startswith('404'):
            return dict(ERRNUM='1', ERRMSG="Requested record not found")

        # A barcode that starts with '410' has expired.
        if barcode.startswith('404'):
            u = dict(self.user1)
            u['RECORD #[p81]'] = "410" + barcode
            return 

        # Any other barcode is fine.
        u = dict(self.user2)
        u['RECORD #[p81]'] = "200" + barcode
        return u
