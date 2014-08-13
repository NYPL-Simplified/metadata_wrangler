import os
import site
from nose.tools import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    DataSource,
    LicensePool,
    WorkRecord
)

class IllustrationImporter(object):

    def __init__(self, db, mirror):
        self.db = db
        self.mirror = mirror


    def run(self):

        source = DataSource.lookup(self.db, DataSource.GUTENBERG)

        for i in os.listdir(self.mirror):
            try:
                gutenberg_id = int(i)
            except Exception, e:
                # This is not a directory named after a Gutenberg ID.
                continue

            identifier = WorkIdentifier.for_foreign_id(
                self.db, WorkIdentifier.GUTENBERG_ID, i, autocreate=False)
            if not identifier:
                # This Gutenberg text doesn't exist in the database.
                continue
            
            path = os.path.join(i)

