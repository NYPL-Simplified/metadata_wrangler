import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from classification import (
    AssignSubjectsToGenres,
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
        else:
            type_restriction = sys.argv[1]

    AssignSubjectsToGenres(production_session()).run(type_restriction, force)
