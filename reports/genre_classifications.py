import os
import site
import sys
import datetime
import csv
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from collections import (
    Counter,
    defaultdict,
)

from integration.threem import (
    ThreeMBibliographicMonitor,
)
from model import (
    DataSource,
    Edition,
    production_session,
    WorkGenre,
    Work,
)
import classifier

def count_for_each_data_source(base_query, sources):
    by_source = dict()
    for source in sources:
        q = base_query.join(Work.primary_edition).filter(Edition.data_source==source)
        by_source[source] = q.count()
    return by_source

def collect(_db, sources):
    stats = defaultdict(Counter)
    base_query = _db.query(WorkGenre).join(WorkGenre.work).join(
        Work.primary_edition)
    for source in sources:
        q = base_query.filter(Edition.data_source==source)
        for classification in q:
            genre = classification.genre.name
            genredata = classifier.genres[genre]
            parentage = [x.name for x in genredata.parents] + [genre]
            parentage.reverse()
            while len(parentage) < 3:
                parentage.append("")
            stats[tuple(parentage)][source] += 1
    return stats

if __name__ == '__main__':

    _db = production_session()

    out = csv.writer(sys.stdout)

    sources = [DataSource.lookup(_db, x) for x in [
        DataSource.GUTENBERG, DataSource.OVERDRIVE, DataSource.THREEM]]
    out.writerow(["Classification", "Parent", "Grandparent"] + [x.name for x in sources] + ["Total"])

    for audience in "Adult", "Young Adult", "Children":
        base_query = _db.query(Work).filter(Work.audience==audience)
        by_source = count_for_each_data_source(base_query, sources)
        
        row = [by_source[source] for source in sources]
        row += [sum(row)]
        row = [audience, "" ,""] + row
        out.writerow(row)

    out.writerow([])
    for fiction, name in (True, "Fiction"), (False, "Nonfiction"), (None, "No Fiction Status"):
        base_query = _db.query(Work).filter(Work.fiction==fiction)
        by_source = count_for_each_data_source(base_query, sources)
        row = [by_source[source] for source in sources]
        row += [sum(row)]
        row = [name, "", ""] + row
        out.writerow(row)

        unclassified_query = Work.with_no_genres(base_query)
        by_source = count_for_each_data_source(unclassified_query, sources)
        row = [by_source[source] for source in sources]
        row += [sum(row)]
        row = [name + ", no genre", "", ""] + row
        out.writerow(row)
    out.writerow([])

    stats = collect(_db, sources)
    for parentage, by_source in sorted(stats.items()):
        total = 0
        row = list(parentage)
        for source in sources:
            row.append(by_source[source])
            total += by_source[source]
        row += [total]
        out.writerow(row)
    
