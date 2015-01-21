import gzip
import os
import re
from nose.tools import set_trace

class FASTNames(dict):

    SUBDIR = "FAST"

    triple_re = re.compile('^<http://id.worldcat.org/fast/([0-9]+)> <http://schema.org[#/]name> "([^"]+)"')

    def load_filehandle(self, fh):
        for triple in fh:
            triple = triple.strip()
            g = self.triple_re.search(triple)
            if not g:
                continue
            identifier, name = g.groups()
            self[identifier] = name

    @classmethod
    def from_data_directory(cls, data_directory):
        my_directory = os.path.join(data_directory, cls.SUBDIR)
        names = FASTNames()
        for i in os.listdir(my_directory):
            if not i.endswith(".nt.gz") and not i.endswith(".nt"):
                continue
            path = os.path.join(my_directory, i)
            print "Loading %s" % path
            names.load_filehandle(gzip.open(path))
            print "There are now %d names." % len(names)
        return names
