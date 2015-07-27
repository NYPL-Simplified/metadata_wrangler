import gzip
import csv
import logging
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
        names = cls()
        consolidated_file = os.path.join(my_directory, "consolidated.csv.gz")
        a = time.time()
        if os.path.exists(consolidated_file):
            logging.info("Reading cached %s names from %s",
                cls.SUBDIR, consolidated_file)
            input_file = gzip.open(consolidated_file)
            reader = csv.reader(input_file)
            for k, v in reader:
                names[k] = v
        else:
            for i in os.listdir(my_directory):
                if not i.endswith(".nt.gz") and not i.endswith(".nt"):
                    continue
                path = os.path.join(my_directory, i)
                logging.info("Loading %s names from %s", cls.SUBDIR, path)
                names.load_filehandle(gzip.open(path))
                logging.info(
                    "There are now %d %s names.", cls.SUBDIR, len(names))

            output = gzip.open(consolidated_file,"w")
            writer = csv.writer(output)
            for k,v in names.items():
                writer.writerow([k, v])
            output.close()
        b = time.time()
        logging.info("Done loading %s names in %.1f sec", cls.SUBDIR, b-a)
        return names

class LCSHNames(FASTNames):

    # TODO: This doesn't work on the childrens' subject classifications;
    # we need to do something closer to real RDF work for those.

    SUBDIR = "LCSH"
    triple_re = re.compile('^<http://id.loc.gov/authorities/[a-zA-Z]+/([a-z]+[0-9]+)> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> "([^"]+)"@en')
