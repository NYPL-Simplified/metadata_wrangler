import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    Subject,
)
from model import production_session

if __name__ == '__main__':
    if len(sys.argv) < 2:
        force = False
        type_restriction=None
    else:
        force = True
        if sys.argv[1] == 'force':
            type_restriction = None
            if len(sys.argv) > 2:
                type_restriction = sys.argv[2]
        else:
            type_restriction = sys.argv[1]

    Subject.assign_to_genres(production_session(), type_restriction, force)
