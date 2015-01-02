import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    production_session,
    ImageScaler,
)
from integration.threem import ThreeMCoverImageMirror
from integration.overdrive import OverdriveCoverImageMirror
# from integration.content_cafe import ContentCafeMirror

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    force = (len(sys.argv) == 3 and sys.argv[2] == 'force')
    mirrors = [OverdriveCoverImageMirror, ThreeMCoverImageMirror]
    ImageScaler(production_session(), path, mirrors).run(
        destination_width=200, destination_height=300, force=force)
