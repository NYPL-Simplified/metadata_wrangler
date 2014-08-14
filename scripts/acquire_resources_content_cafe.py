import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.content_cafe import (
    ContentCafeMirror,
)
from model import production_session

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: %s [data storage directory]" % sys.argv[0]
        sys.exit()
    path = sys.argv[1]      
    userid = os.environ['CONTENT_CAFE_USERNAME']
    password = os.environ['CONTENT_CAFE_PASSWORD']
    ContentCafeMirror(production_session(), path, userid, password).run()
