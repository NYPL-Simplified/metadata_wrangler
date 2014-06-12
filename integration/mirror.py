from nose.tools import set_trace
import os
import random
import requests
import urlparse
import md5

class Mirror(object):

    FOR_HOSTNAME = dict()

    DIRECTORY_NAME = "mirror"

    def __init__(self, data_directory):
        self.data_directory = os.path.join(data_directory, self.DIRECTORY_NAME)
        if not os.path.exists(self.data_directory):
            raise ValueError("Data directory %s does not exist." % 
                             self.data_directory)

    def local_path(self, url):
        parsed = urlparse.urlparse(url)
        netloc = parsed.netloc
        path = parsed.path
        if path.startswith("/"):
            path = path[1:]
        if '/.' in path:
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

    def download(self, url, local_path, request_headers):
        response = requests.get(url, headers=request_headers)
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


        return random.random()
