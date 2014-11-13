import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.overdrive import (
    OverdriveCoverImageMirror,
)
from model import production_session

if __name__ == '__main__':
    OverdriveCoverImageMirror(production_session()).run()
