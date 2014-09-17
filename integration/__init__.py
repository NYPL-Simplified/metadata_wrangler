import os
from lxml import etree

class XMLParser(object):

    """Helper functions to process XML data."""

    @classmethod
    def _xpath(cls, tag, expression):
        """Wrapper to do a namespaced XPath expression."""
        return tag.xpath(expression, namespaces=cls.NAMESPACES)

    @classmethod
    def _xpath1(cls, tag, expression):
        """Wrapper to do a namespaced XPath expression."""
        values = cls._xpath(tag, expression)
        if not values:
            return None
        return values[0]

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
        if isinstance(xml, basestring):
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

    def __init__(self, cache_directory, subdir_chars=None):
        self.cache_directory = cache_directory
        self.subdir_chars = subdir_chars
        if not os.path.exists(self.cache_directory):
            os.makedirs(self.cache_directory)

    def _filename(self, key):
        if len(key) > 140:
            key = key[:140]
        if self.subdir_chars:
            subdir = key[:self.subdir_chars]
            directory = os.path.join(self.cache_directory, subdir)
        else:
            directory = self.cache_directory
        return os.path.join(directory, key)

    def exists(self, key):
        return os.path.exists(self._filename(key))

    def open(self, key):
        return open(self._filename(key))

    def store(self, key, value):
        filename = self._filename(key)
        if self.subdir_chars:
            # Make sure the subdirectory exists.
            directory = os.path.split(filename)[0]
            if not os.path.exists(directory):
                os.makedirs(directory)
        f = open(filename, "w")
        f.write(value)
        f.close()
        return filename
