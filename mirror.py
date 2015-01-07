from nose.tools import set_trace
import gzip
import os
import random
import urlparse
import requests

from core.model import (
    DataSource,
    Resource,
)
from core.s3 import S3Uploader

class FilesystemCache(object):

    """A simple filesystem-based cache for HTTP representations."""

    def __init__(self, cache_directory, subdir_chars=None,
                 substring_from_beginning=True,
                 check_subdirectories=False, compress=False):
        self.cache_directory = cache_directory
        self.subdir_chars = subdir_chars
        self.substring_from_beginning = substring_from_beginning
        if not os.path.exists(self.cache_directory):
            os.makedirs(self.cache_directory)
        self.substring_from_beginning = substring_from_beginning
        self.check_subdirectories = check_subdirectories or subdir_chars
        self.compress = compress

    def _filename(self, key):
        if len(key) > 140:
            key = key[:140]
        if self.subdir_chars:
            if self.substring_from_beginning:
                subdir = key[:self.subdir_chars]
            else:
                subdir = key[-self.subdir_chars:]
            directory = os.path.join(self.cache_directory, subdir)
        else:
            directory = self.cache_directory
        return os.path.join(directory, key)

    def exists(self, key):
        return os.path.exists(self._filename(key))

    @property
    def _open(self):
        if self.compress:
            f = gzip.open
        else:
            f = open
        return f
            
    def open(self, key):
        return self._open(self._filename(key))

    def store(self, key, value):
        filename = self._filename(key)
        if self.check_subdirectories:
            # Make sure the subdirectory exists.
            directory = os.path.split(filename)[0]
            if not os.path.exists(directory):
                os.makedirs(directory)
        f = self._open(filename, "w")
        f.write(value)
        f.close()
        return filename

class CoverImageMirror(object):
    """Downloads images via HTTP and writes them to disk."""

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
        self.data_source = DataSource.lookup(self._db, self.DATA_SOURCE)
        self.original_subdir = self.data_directory(data_directory)
        self.original_cache = FilesystemCache(self.original_subdir, 3)
        self.uploader = S3Uploader()

    def run(self):
        """Mirror all image resources associated with this data source."""
        q = self._db.query(Resource).filter(
            Resource.rel==Resource.IMAGE).filter(
                Resource.data_source==self.data_source).filter(
                    Resource.mirror_date==None)
        print "Mirroring %d images." % q.count()
        resultset = q.limit(100).all()
        to_upload = []
        while resultset:
            for resource in resultset:
                to_upload.append(self.mirror(resource))

            self.uploader.upload_resources(to_upload)
            self._db.commit()
            resultset = q.limit(100).all()
        self._db.commit()

    types_for_image_extensions = { ".jpg" : "image/jpeg",
                                   ".gif" : "image/gif",
                                   ".png" : "image/png"}

    def filename_for(self, resource):
        href = resource.href
        extension = href[href.rindex('.'):]
        filename = resource.identifier.identifier + extension
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
        return location, resource.final_url
    

class Mirror(object):
    """I'm not sure if this is used..."

    FOR_HOSTNAME = dict()

    DIRECTORY_NAME = "mirror"

    def __init__(self, data_directory, sleep_time=None):
        self.sleep_time = sleep_time or self.default_sleep_time
        if not os.path.exists(data_directory):
            raise ValueError("Base data directory %s does not exist." % 
                             data_directory)

        self.data_directory = os.path.join(data_directory, self.DIRECTORY_NAME)
        if not os.path.exists(self.data_directory):
            os.makedirs(self.data_directory)

    def local_path(self, url):
        parsed = urlparse.urlparse(url)
        netloc = parsed.netloc
        if not netloc:
            return None
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]
        if not path:
            return None
        if '/.' in path or path.startswith("./") or path.startswith("../"):
            return None
        return os.path.join(self.data_directory, netloc, path)

    def ensure_mirrored(self, url, request_headers={}):
        sleep_time = 0
        path = self.local_path(url)
        if not path:
            raise ValueError("Cannot mirror URL due to its structure: %s" % url)
        if not os.path.exists(path):
            d, f = os.path.split(path)
            if not os.path.exists(d):
                os.makedirs(d)
            sleep_time = self.download(url, path, request_headers)
        return path, sleep_time

    def make_request(self, url, headers):
        return requests.get(url, headers=headers)

    def download(self, url, local_path, request_headers={}):
        response = self.make_request(url, request_headers)
        if response.status_code != 200:
            raise Exception(
                "Request to %s got response code %s: %s" % (
                    url, response.status_code, response.content))

        d, f = os.path.split(local_path)
        if not os.path.exists(d):
            os.makedirs(d)
        out = open(local_path, "w")
        out.write(response.content)
        out.close()

        return self.sleep_time(url)

    def default_sleep_time(self, url):
        """How long to sleep after making a request to the given URL."""
        random.random()
