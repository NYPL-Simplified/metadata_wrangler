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
    return int(pg_id) in [
        11, 928, 28885, 23716, 19033, # Alice in Wonderland
        12, 23718,                    # Through The Looking Glass

        76, 19640, 9007,              # The Adventures of Huckleberry Finn
        32325,                        # "The Adventures of Huckleberry Finn,
                                      #  Tom Sawyer's Comrade"

        # This is the best example for two books that have different titles
        # but are the same work.
        15, 9147,                     # Moby Dick
        2701, 2489, 28794,            # "Moby Dick, or, the Whale"

        # This is the best example for two books that have similar titles
        # but are different works.
        91, 9036,                     # Tom Sawyer Abroad
        93, 9037,                     # Tom Sawyer, Detective

        # These aren't really that useful except for verifying that
        # books semi-similar enough to other books don't get
        # consolidated.
        19778,                        # AiW in German
        28371,                        # AiW in French
        17482,                        # AiW in Esperanto
        114,                          # Tenniel illustrations only
        19002,                        # Alice's Adventures Under Ground
        10643,                        # World's Greatest Books, includes AiW
        36308,                        # AiW songs only
        19551,                        # AiW in words of one syllable
        35688,                        # "Alice in Wonderland" but by a
                                      #  different author.
        35990,                        # "The Story of Lewis Carroll"

        7100, 7101, 7102, 7103,       # Huckleberry Finn in 5-chapter
        7104, 7105, 7106, 7107,       #  chunks

        74, 26203, 9038,              # The Adventures of Tom Sawyer
        30165,                        # Tom Sawyer in German
        30890,                        # Tom Sawyer in French
        45333,                        # Tom Sawyer in Finnish
        7193, 7194, 7198, 7196,       # Tom Sawyer in chunks
        7197, 7198, 7199, 7200,
]



def secret_garden_subset(pg_id, archive, archive_item):
    return int(pg_id) in [113, 8812, 17396, 21585,   # The Secret Garden
                          146, 19514, 23711, 37332,  # A Little Princess
                          479, 23710,                # Little Lord Fauntleroy
                          
                          # # Some pretty obscure books.
                          # 2300,
                          # 2400,
                          # 2500,
                          # 2600,
    ]

def first_half_subset(pg_id, archive, archive_item):
    """A large data set containing all the well-known public domain works,
    but not the entirety of Project Gutenberg."""
    return int(pg_id) < 20000

subset = very_small_subset

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    session = production_session()
    GutenbergMonitor(path).run(session, subset)
