import os
import site
import sys
import json
import re
from nose.tools import set_trace
from collections import defaultdict
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    DataSource,
    production_session,
    WorkRecord,
    WorkIdentifier,
)

useless = [
    ', or the London Chiarivari,',
    ', or The London Chiarivari,',
    ', A Novel',
]

stoppers = [': ', '; ',
            ', and Other ', ', And Other ', ' and Other ', ' And Other',
            ' and other', ', and other']

def shorten_title(title):
    orig = title
    for u in useless:
        title = title.replace(u, "")
    for stop_at in stoppers:
        if stop_at in title:
            title = title[:title.index(stop_at)]

    title = title.strip()
    if title == orig:
        return None
    return title

class CoverMatcher(object):

    gutenberg_id = re.compile(".*/([0-9]+)")

    def __init__(self, db, ls_file):
        seen_ids = set()
        self.images_for_work = defaultdict(list)
        data_source = DataSource.lookup(db, DataSource.GUTENBERG)
        gid = container = working_directory = None
        oops = open("/home/leonardr/oops.txt", "w")
        for i in open(ls_file):
            i = i.strip()
            if not i:
                if gid and container:
                    # Look up Gutenberg info.
                    if gid in seen_ids:
                        oops.write(gid + "\n")
                        continue
                    wr = WorkRecord.for_foreign_id(
                        db, data_source, WorkIdentifier.GUTENBERG_ID, gid,
                        create_if_not_exists=False)
                    if wr:
                        short_names = []
                        long_names = []
                        for a in wr.authors:
                            if a.family_name:
                                short_names.append(a.family_name)
                            if a.display_name:
                                long_names.append(a.display_name)
                            else:
                                if name not in ['Various']:
                                    long_names.append(a.name)
                        short_name = ", ".join(sorted(short_names))
                        long_name = ", ".join(sorted(long_names))
                        
                        d = dict(
                            authors_short=short_name,
                            authors_long=long_name,
                            identifier=gid,
                            title=wr.title,
                            title_short=shorten_title(wr.title),
                            subtitle=wr.subtitle,
                            identifier_type = "Gutenberg ID",
                            illustrations=container,
                        )
                        print json.dumps(d)
                        seen_ids.add(gid)

                    gid = container = working_directory = None
            elif i.endswith("images:"):
                working_directory = i[:-1]
                gid = self.gutenberg_id.search(i)
                if gid:
                    gid = gid.groups()[0]
                    container = self.images_for_work[gid]
            elif container is not None:
                container.append(os.path.join(working_directory, i))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [path to ls-R]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    CoverMatcher(production_session(), path)
