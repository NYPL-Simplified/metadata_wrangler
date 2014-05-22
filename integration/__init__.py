class XMLParser(object):

    """Helper functions to process XML data."""

    def text_of_optional_subtag(self, tag, name):
        tag = tag.xpath(name)
        if tag:
            return tag[0].text
        return None
      
    def text_of_subtag(self, tag, name):
        return tag.xpath(name)[0].text

    def int_of_subtag(self, tag, name):
        return int(self.text_of_subtag(tag, name))

    def process_all(self, xml, xpath, namespaces={}, handler=None):
        if not handler:
            handler = self.process_one
        if isinstance(xml, str):
            root = etree.fromstring(xml)
        else:
            root = xml
        for i in root.xpath(xpath, namespaces=namespaces):
            data = handler(i, namespaces)
            if data:
                yield data

    def process_one(self, tag, namespaces):
        return None


class FilesystemCache(object):

    """A simple filesystem-based cache for HTTP representations."""

    def __init__(self, cache_directory):
        self.cache_directory = cache_directory
        if not os.path.exists(self.cache_directory):
            os.makedirs(d)

    def _filename(self, key):
        if len(key) > 140:
            key = key[:140]
        return os.path.join(self.cache_directory, key)

    def exists(self, key):
        return os.path.exists(self._filename(key))

    def open(self, key):
        return open(self._filename(key))

    def store(self, key, value):
        f = open(self._filename(key), "w")
        f.write(value)
        f.close()
    
