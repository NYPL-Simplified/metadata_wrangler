import pkgutil

from integration.millenium_patron import MilleniumPatronAPI

class DummyAPI(MilleniumPatronAPI):

    class DummyResponse(object):
        def __init__(self, body):
            self.body = body

    queue = []

    def queue(self, filename):
        data = pkgutil.get_data(
            "tests.integration",
            "files/millenium_patron/%s" % filename)
        queue.append(data)

    def request(self, **kwargs):
        return DummyResponse(queue.pop())


class TestMilleniumPatronAPI(object):

    def test_dump_no_such_barcode(self):
        self.api.queue("dump.no such barcode.html")
        eq_(None, self.api.dump("bad barcode"))

    def test_dump_success(self):
        self.api.queue("dump.success.html")
        response = self.api.dump("good barcode")
        # TODO: what do we actually want here?

    def test_pintest_no_such_barcode(self):
        self.api.queue("pintest.no such barcode.html")
        eq_(True, self.api.pintest("wrong barcode", "pin"))

    def test_pintest_wrong_pin(self):
        self.api.queue("pintest.bad.html")
        eq_(False, self.api.pintest("barcode", "wrong pin"))

    def test_pintest_right_pin(self):
        self.api.queue("pintest.bad.html")
        eq_(True, self.api.pintest("barcode", "right pin"))
