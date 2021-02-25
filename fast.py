"""Load English-language names for FAST and LCSH subject classifications
from N-Triple files acquired from data dumps.

This is how we know, e.g. that FAST classification 1750175 means
"Short stories, American".
"""
from contextlib import contextmanager
import gzip
import csv
from io import BytesIO
import logging
import os
import re
from nose.tools import set_trace
import time
import zipfile

class FASTNames(dict):

    SUBDIR = "FAST"

    triple_re = re.compile('^<http://id.worldcat.org/fast/([0-9]+)> <http://schema.org[#/]name> "([^"]+)"')

    @classmethod
    def from_data_directory(cls, data_directory):
        """Load names from a directory that either contains a bunch of
        files in N-Triples format or a single consolidated CSV file.

        The first call will run very slowly because it involves a lot
        of regular expression work. Once that completes, a CSV file
        containing consolidated data will be written to
        `data_directory`. Subsequent calls will read from that file
        and run much more quickly.
        """
        my_directory = os.path.join(data_directory, cls.SUBDIR)
        consolidated_file = os.path.join(my_directory, "consolidated.csv.gz")
        a = time.time()
        if os.path.exists(consolidated_file):
            # A consolidated file has already been created. Load it --
            # it's quick.
            names = cls.from_consolidated_file(consolidated_file)
        else:
            # We have to go through a bunch of N-Triples files.
            names = cls()
            for i in sorted(os.listdir(my_directory)):
                path = os.path.join(my_directory, i)
                logging.info("Loading %s names from %s", cls.SUBDIR, path)
                names.load_triples_file(path)
                logging.info(
                    "There are now %d %s names.", len(names), cls.SUBDIR
                )

            # Now that we've done that, write out a consolidated file
            # so next time will go more quickly.
            names.write_consolidated_file(consolidated_file)
        b = time.time()
        logging.info(
            "Loaded %d %s names in %.1f sec", len(names), cls.SUBDIR, (b-a)
        )
        return names

    def load_triples_file(self, path):
        """Load classifications from an N-Triples file."""
        if path.endswith(".nt.gz"):
            # This is a single GZipped N-Triples file.
            self.load_triples_filehandle(gzip.open(path, 'rb'))
        elif path.endswith(".nt.zip"):
            # This is a ZIP file containing one or more (probably just
            # one) N-Triples files. Load each one the zip.
            for fh in self.triples_filehandles_from_zip(path):
                self.load_triples_filehandle(fh)
        else:
            # This is some other kind of file. Do nothing.
            pass

    def triples_filehandles_from_zip(self, path):
        """Open up `path` as a ZIP file and find one or more (probably just
        one) N-Triples files inside.

        :yield: A BytesIO for each N-Triples file in the ZIP.
        """
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                if name.endswith(".nt"):
                    yield BytesIO(archive.read(name))

    def load_triples_filehandle(self, fh):
        """Load a number of N-Triples from a filehandle, and
        keep track of any identifier-name mappings found.
        """
        for triple in fh:
            identifier, name = self.extract_identifier_and_name(triple)
            if identifier and name:
                self[identifier] = name

    def extract_identifier_and_name(self, triple):
        """Extract an identifier and a name from a single line of an N-Triples
        file.
        """
        triple = triple.strip()
        g = self.triple_re.search(triple)
        if not g:
            return None, None
        return g.groups()

    @classmethod
    def from_consolidated_file(cls, path):
        """Load classifications from a CSV file, generated by an
        earlier call to write_consolidated_file().
        """
        logging.info(
            "Reading cached %s names from %s", cls.SUBDIR, path
        )
        names = cls()
        fh = gzip.open(path, 'rb')
        reader = csv.reader(fh)
        for identifier, name in reader:
            names[identifier] = name
        return names

    def write_consolidated_file(self, path):
        """Write a CSV file containing information consolidated
        from several N-Triples files.
        """
        with self.consolidated_output_filehandle(path) as output:
            writer = csv.writer(output)
            for k,v in list(self.items()):
                writer.writerow([k, v])

    @contextmanager
    def consolidated_output_filehandle(self, path):
        """Open a write filehandle to the given path.

        This method is designed to be mocked in unit tests.
        """
        with gzip.open(path, "wb") as out:
            yield out


class LCSHNames(FASTNames):

    # TODO: This doesn't work on the childrens' subject classifications;
    # we need to do something closer to real RDF work for those.

    SUBDIR = "LCSH"
    triple_re = re.compile('^<http://id.loc.gov/authorities/[a-zA-Z]+/([a-z]+[0-9]+)> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> "([^"]+)"@en')
