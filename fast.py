import gzip
import csv
import os
import re
from nose.tools import set_trace
import time

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
        consolidated_file = os.path.join(my_directory, "consolidated.csv.gz")
        a = time.time()
        if os.path.exists(consolidated_file):
            print "Reading cached FAST names from %s" % consolidated_file
            input_file = gzip.open(consolidated_file)
            reader = csv.reader(input_file)
            for k, v in reader:
                names[k] = v
        else:
            for i in os.listdir(my_directory):
                if not i.endswith(".nt.gz") and not i.endswith(".nt"):
                    continue
                path = os.path.join(my_directory, i)
                print "Loading %s" % path
                names.load_filehandle(gzip.open(path))
                print "There are now %d names." % len(names)

            writer = csv.writer(gzip.open(consolidated_file,"w"))
            for k,v in names.items():
                writer.writerow([k, v])
        b = time.time()
        print "Done loading FAST names in %.1f sec" % (b-a)
        return names
