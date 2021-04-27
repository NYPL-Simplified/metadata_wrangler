import os
import site
import sys
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    create,
    get_one_or_create,
    production_session,
    Subject,
    Identifier,
)

def process_file(_db, filename, class_):
    a = 0
    for i in open(filename):
        v = i.strip().split("\t")
        if class_ == Subject and len(v) == 3:
            type, identifier, name = v
        elif len(v) == 2:
            type, identifier = v
            name = None
        else:
            print("Bad data: %r" % i)
        args = {}
        if class_ == Subject and name:
            args['name'] = name
        get_one_or_create(
            _db, class_, type=type, identifier=identifier,
            create_method_kwargs=args
        )
        a += 1
        if not a % 1000:
            _db.commit()
            print(a, class_.__name__)
    _db.commit()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: %s [data storage directory]" % sys.argv[0])
        sys.exit()
    path = sys.argv[1]      
    _db = production_session()
    seed_dir = os.path.join(path, "seed")
    #subjects_path = os.path.join(seed_dir, "subjects.tsv")
    #process_file(_db, subjects_path, Subject)

    identifiers_path = os.path.join(seed_dir, "identifiers.tsv")
    process_file(_db, identifiers_path, Identifier)
