import os
import site
import sys
import json
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))

from nose.tools import set_trace

from collections import Counter
from model import (
    production_session,
    Resource,
    DataSource,
    Work,
    LicensePool,
    WorkIdentifier,
)

class ClassicFinder(object):

    def __init__(self, _db):
        self._db = _db
        self.gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.gutenberg_cover_generator = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR)

    def dump(self, output, limit=1000):
        # We don't use .limit() because a work that includes two
        # Gutenberg license pools counts twice towards the limit.
        q = self._db.query(Work).join(Work.license_pools).filter(
            LicensePool.data_source==self.gutenberg).order_by(
                Work.quality.desc())
        count = 0
        print "Dumping %s of %d" % (limit, q.count())
        for i in q:
            data = self.work_to_dict(i)
            json.dump(data, output)
            output.write("\n")
            output.flush()
            count += 1
            print count, data['title']
            if count >= limit:
                break

    def work_to_dict(self, work):
        title = work.title
        author_string = work.authors

        data = dict()
        data['title'] = title
        data['genres'] = [g.name for g in work.genres]
        data['audience'] = work.audience
        data['fiction'] = work.fiction
        data['popularity'] = work.quality
        data['language'] = work.language

        gutenberg_texts = []
        for lp in work.license_pools:
            if lp.data_source != self.gutenberg:
                continue
            wr = lp.work_record()
            one_text = dict(
                identifier=[lp.identifier.type, lp.identifier.identifier],
                title=wr.title,
                )
            if not 'authors' in data:
                data['authors'] = []
                for author in wr.authors:
                    data['authors'].append(
                        dict(name=author.name,
                             display_name=author.display_name,
                             family_name=author.family_name,
                             wikipedia_name=author.wikipedia_name))
            gutenberg_texts.append(one_text)
        data['gutenberg_texts'] = gutenberg_texts

        primary_identifier_ids = [
            x.primary_identifier.id for x in work.work_records]
        ids = WorkIdentifier.recursively_equivalent_identifier_ids(
            self._db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = WorkIdentifier.flatten_identifier_ids(ids)

        subjects = Counter()
        classifications = WorkIdentifier.classifications_for_identifier_ids(
            self._db, flattened_data)
        for c in classifications:
            subject = c.subject
            subjects[subject] += c.weight

        flattened_subjects = []
        data['subjects'] = flattened_subjects
        for subject, weight in subjects.most_common():
            d = dict(
                type=subject.type, identifier=subject.identifier,
                weight=weight)
            flattened_subjects.append(d)
            if subject.name:
                d['name'] = subject.name
            if subject.genre:
                d['genre'] = subject.genre.name
            if subject.fiction:
                d['fiction'] = subject.fiction
            if subject.audience:
                d['audience'] = subject.audience

        summary_counter = Counter()
        summaries = []
        data['summaries'] = summaries
        summary_resources = WorkIdentifier.resources_for_identifier_ids(
            self._db, flattened_data, Resource.DESCRIPTION).filter(
                Resource.content != None)
        for r in summary_resources:
            summary_counter[(r.content, r.quality)] += 1
        for (content, quality), count in summary_counter.most_common():
            summaries.append(dict(content=content, quality=quality,
                                  weight=count))


        covers = []
        data['covers'] = covers
        cover_resources = WorkIdentifier.resources_for_identifier_ids(
            self._db, flattened_data, Resource.IMAGE).filter(
                Resource.data_source_id.in_(
                    [self.gutenberg_cover_generator.id, self.gutenberg.id]))
        for cover in cover_resources:
            covers.append(cover.final_url)
        return data


if __name__ == '__main__':
    ClassicFinder(production_session()).dump(open(sys.argv[1], "w"))
