import os
import site
import sys
from pdb import set_trace
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from model import (
    WorkRecord,
    WorkIdentifier
)
from integration.gutenberg import PopularityScraper
from model import production_session

if __name__ == '__main__':
    session = production_session()
    PopularityScraper().scrape()
