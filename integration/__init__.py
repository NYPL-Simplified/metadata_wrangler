import os
import urlparse

import requests
from lxml import etree
from nose.tools import set_trace


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


class CoverImageMirror(object):
    """Downloads images from Overdrive and writes them to disk."""

    COVERS_DIR = "covers"
    ORIGINAL_SUBDIR = "original"
    SCALED_SUBDIR = "scaled"

    ORIGINAL_PATH_VARIABLE = None
    SCALED_PATH_VARIABLE = None
    DATA_SOURCE = None

    @classmethod
    def data_directory(self, base_data_directory):
        return os.path.join(base_data_directory, self.DATA_SOURCE, 
                            self.COVERS_DIR, self.ORIGINAL_SUBDIR)

    @classmethod
    def scaled_image_directory(self, base_data_directory):
        return os.path.join(base_data_directory, self.DATA_SOURCE,
                            self.COVERS_DIR, self.SCALED_SUBDIR)

    def __init__(self, db, data_directory):
        self._db = db
        from model import DataSource
        self.data_source = DataSource.lookup(self._db, self.DATA_SOURCE)
        self.original_subdir = self.data_directory(data_directory)
        self.original_cache = FilesystemCache(self.original_subdir, 3)

    def run(self):
        """Mirror all image resources associated with this data source."""
        from model import Resource
        q = self._db.query(Resource).filter(
            Resource.rel==Resource.IMAGE).filter(
                Resource.data_source==self.data_source).filter(
                    Resource.mirror_date==None)
        print "Mirroring %d images." % q.count()
        resultset = q.limit(100).all()
        while resultset:
            for resource in resultset:
                self.mirror(resource)
            self._db.commit()
            resultset = q.limit(100).all()
        self._db.commit()

    types_for_image_extensions = { ".jpg" : "image/jpeg",
                                   ".gif" : "image/gif",
                                   ".png" : "image/png"}

    def filename_for(self, resource):
        href = resource.href
        extension = href[href.rindex('.'):]
        filename = resource.work_identifier.identifier + extension
        return filename

    def mirror(self, resource):
        filename = self.filename_for(resource)
        if self.original_cache.exists(filename):
            content_type = self.types_for_image_extensions.get(
                filename, "image/jpeg")
            data = self.original_cache.open(filename).read()
            location = self.original_cache._filename(filename)
            network = False
        else:
            response = requests.get(resource.href)
            if response.status_code != 200:
                resource.could_not_mirror()
                return
            content_type = response.headers['Content-Type']
            data = response.content
            location = self.original_cache.store(filename, data)
            network = True
        path = "%(" + self.ORIGINAL_PATH_VARIABLE + ")s" + location[len(self.original_subdir):]
        if network:
            print "%s => %s" % (resource.href, path)
        else:
            print "CACHE %s" % path
        resource.mirrored_to(path, content_type, data)
