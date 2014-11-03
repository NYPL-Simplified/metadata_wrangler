"""Gather subject classifications and popularity measurements from
Gutenberg's 'bookshelf' wiki.
"""
import os
import site
import sys
from nose.tools import set_trace

d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    production_session,
)
from integration.gutenberg import GutenbergBookshelfClient

if __name__ == '__main__':
    db = production_session()
    GutenbergBookshelfClient(db).full_update()
    db.commit()
