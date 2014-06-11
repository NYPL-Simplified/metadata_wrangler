import requests
import urlparse
import md5

class Mirror(object):

    FOR_HOSTNAME = dict()

    def __init__(self, data_directory):
        self.data_directory = data_directory

    @classmethod
    def hostname_key(cls, netloc):
        return ".".join(netloc.split(".")[-2:]) 

    @classmethod
    def ensure_mirrored(cls, url):
        parsed = urlparse.urlparse(url)
        netloc = parsed.netloc
        key = cls.hostname_key(netloc)

        handler = cls.FOR_HOSTNAME.get(key, None)

        if not handler:
            raise ValueError("No handler registered for %s" % url)



        path = parsed.path

    def save(response, path)


class MirrorHandler(object):

    def ensure_mirrored(cls, mirror, url, key, parsed):
        mirror.save(url, key, path)

    def path(mirror, url, key, parsed):
        return parsed.path
