# A one-off script to move links from WorkRecord.links to the Resources table.
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

class LinksConverter(object):

    def __init__(self, db):
        self.db = db

    def run(self):
        source = DataSource.lookup(self.db, DataSource.GUTENBERG)
        a = 0
        found_some = True
        while found_some:
            found_some = False
            for wr in self.db.query(WorkRecord).filter(WorkRecord.links != None).filter(WorkRecord.data_source==source).limit(1000):
                if wr.links is None:
                    print "Should not happen."
                    continue
                found_some = True
                try:
                    pool = self.db.query(LicensePool).filter(
                        LicensePool.data_source==wr.data_source).filter(
                            LicensePool.identifier==wr.primary_identifier).one()
                except Exception, e:
                    set_trace()
                l = dict(wr.links)
                for rel, links in l.items():
                    for link in links:
                        href = link['href']
                        media_type = None
                        if 'type' in link:
                            media_type = link['type']
                        r, new = pool.add_resource(rel, href, source, media_type)
                wr.links = None
                a += 1
            self.db.commit()
            print a

from model import production_session
session = production_session()
LinksConverter(session).run()
