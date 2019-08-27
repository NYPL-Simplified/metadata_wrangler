from nose.tools import set_trace

class MockOCLCClassifyAPI(object):
    def __init__(self):
        self.results = []

    def queue_lookup(self, *results):
        self.results += results

    def lookup_by(self, **kwargs):
        return self.results.pop(0)


class MockOCLCLinkedDataAPI(object):

    def __init__(self):
        self.info_results = []

    def queue_info_for(self, *metadatas):
        self.info_results.append(metadatas)

    def info_for(self, *args, **kwargs):
        return self.info_results.pop(0)


class MockVIAFClient(object):

    def __init__(self):
        self.results = []
        self.viaf_lookups = []
        self.name_lookups = []

    def queue_lookup(self, *results):
        self.results += results

    def lookup_by_viaf(self, *args, **kwargs):
        self.viaf_lookups.append((args, kwargs))
        return self.results.pop(0)
    
    def lookup_by_name(self, *args, **kwargs):
        self.name_lookups.append((args, kwargs))
        return self.results.pop(0)
