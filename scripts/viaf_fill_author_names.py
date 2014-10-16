import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.viaf import (
    VIAFClient,
)
from model import production_session

if __name__ == '__main__':
    force = False
    if len(sys.argv) > 1 and sys.argv[1] == 'force':
        force = True
    VIAFClient(production_session()).run(force)
