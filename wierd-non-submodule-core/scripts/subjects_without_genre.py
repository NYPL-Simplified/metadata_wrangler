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
        type_restriction=None
    else:
        type_restriction = sys.argv[1]

    q = Subject.common_but_not_assigned_to_genre(
        production_session(), type_restriction=type_restriction,
        min_occurances=1000)
    for s in q.limit(10000):
        print s
