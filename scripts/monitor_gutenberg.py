import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.gutenberg import (
    GutenbergAPI,
    GutenbergMonitor,
)
from model import production_session

# Some useful implementations of subset() 

def very_small_subset(pg_id, archive, archive_item):
    """A minimal set of test data that focuses on the many Gutenberg
    editions of three works: "Moby-Dick", "Alice in Wonderland", and
    "The Adventures of Huckleberry Finn".
    """
    return int(pg_id) in [11, 19033, 28885, 928, 19778, 19597, 28371, 17482, 23716, 114, 19002, 10643, 36308, 19551, 35688, 35990, 2701, 15, 2489, 28794, 9147, 76, 32325, 19640, 9007, 7100, 7101, 7102, 7103, 7104, 7105, 7106, 7107, 74, 30165, 26203, 93, 7193, 91, 7194, 7198, 9038, 7195, 30890, 7196, 7197, 45333, 7199, 7200, 9037, 9036, 12, 23718]

def first_half_subset(pg_id, archive, archive_item):
    """A large data set containing all the well-known public domain works,
    but not the entirety of Project Gutenberg."""
    return int(pg_id) < 20000

subset = None

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    session = production_session()
    GutenbergMonitor(path).run(session, subset)
