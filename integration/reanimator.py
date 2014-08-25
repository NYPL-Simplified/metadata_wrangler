"""Generate data sets for the Project Gutenberg Reanimator."""

from nose.tools import set_trace
import os
import json
from collections import defaultdict
from model import (
    DataSource,
    Resource,
    WorkRecord,
    WorkIdentifier,
)
from integration.oclc import (
    OCLCLinkedData,
)
from sqlalchemy.orm import lazyload
from classification import Classification

from util import LanguageCodes

def identifier_key(identifier):
    if identifier.type == WorkIdentifier.OCLC_WORK:
        return "owi-%s" % identifier.identifier
    elif identifier.type == WorkIdentifier.GUTENBERG_ID:
        return "Gutenberg-%s" % identifier.identifier
    elif identifier.type == WorkIdentifier.OCLC_NUMBER:
        return "OCLC-%s" % identifier.identifier
    elif identifier.type == WorkIdentifier.ISBN:
        return "ISBN-%s" % identifier.identifier
    else:
        set_trace()


class GutenbergReanimator(object):

    def __init__(self, _db, data_directory):
        self._db = _db
        self.oclc_ld = OCLCLinkedData(data_directory)

    def dump(self, output_directory):
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        gutenberg_output_path = os.path.join(output_directory, "gutenberg.json")
        oclc_work_output_path = os.path.join(output_directory, "oclc_work.json")
        oclc_edition_output_path = os.path.join(output_directory, "oclc_edition.json")
        equivalency_output_path = os.path.join(output_directory, "equivalency.json")

        seen_gutenberg = set()
        if os.path.exists(gutenberg_output_path):
            for i in open(gutenberg_output_path):
                data = json.loads(i.strip())
                seen_gutenberg.add(data['id'])

        exclude = [x[len("Gutenberg-"):] for x in seen_gutenberg]

        gutenberg_output = open(gutenberg_output_path, "a")
        oclc_work_output = open(oclc_work_output_path, "a")
        oclc_edition_output = open(oclc_edition_output_path, "a")
        equivalency_output = open(equivalency_output_path, "a")

        gutenberg_work_records = self._db.query(WorkRecord).join(
            WorkRecord.primary_identifier).filter(
                WorkIdentifier.type==WorkIdentifier.GUTENBERG_ID).filter(
                    ~WorkIdentifier.identifier.in_(exclude))

        for g in gutenberg_work_records:
            id = "Gutenberg-%s" % g.primary_identifier.identifier
            if id in seen_gutenberg:
                continue

            # Generate the Gutenberg record. This includes a link to
            # an epub, a link to an HTML version, links to covers, and
            # descriptions.
            record = self.gutenberg_record(g, id)

            # Now generate lists of equivalencies.
            work_obj = g.work
            if work_obj:
                for wr in work_obj.work_records:
                    id_key = identifier_key(wr.primary_identifier)
                    if id == id_key:
                        continue
                    similarity = work_obj.similarity_to(wr)
                    self.add_equivalency(
                        equivalency_output, id, 
                        id_key, similarity)

                    if wr.primary_identifier.type == WorkIdentifier.OCLC_WORK:
                        work = dict(
                            id=id_key,
                            title=wr.title,
                            authors=self.author_list(wr.authors),
                        )

                        json.dump(work, oclc_work_output)
                        oclc_work_output.write("\n")
                    elif wr.primary_identifier.type == WorkIdentifier.GUTENBERG_ID:
                        # Do nothing. The other Gutenberg ID will have
                        # its turn.
                        pass
                    else:
                        set_trace()
                        pass

            json.dump(record, gutenberg_output)
            gutenberg_output.write("\n")
            for i in (
                    oclc_edition_output, oclc_work_output, gutenberg_output,
                    equivalency_output):
                i.flush()

    def add_equivalency(self, out, id1, id2, confidence):
        d = [id1, id2, confidence]
        json.dump(d, out)
        out.write("\n")

    def ratio(self, a, b):
        if not a and not b:
            return None
        return float(a)/(a+b)

    def adult_audience_confidence(self, work):
        a = work.subjects.get('audience', {})
        children = a.get(Classification.AUDIENCE_CHILDREN, 0)
        adult = a.get(Classification.AUDIENCE_ADULT, 0)
        return self.ratio(adult, children)

    def fiction_confidence(self, work):
        a = work.subjects.get('fiction', {})
        yes = a.get("True", 0)
        no = a.get("False", 0)
        return self.ratio(yes, no)

    def subjects(self, wr):
        headings = defaultdict(list)
        for i in ('LCC', 'LCSH', 'FAST', 'DDC'):
            for heading in wr.subjects.get(i, []):
                if 'value' in heading:
                    v = heading['value']
                else:
                    v = heading['id']
                headings[i].append(v)
        return headings

    def author_list(self, contributors):
        authors = []
        for a in contributors:
            author = dict(name=a.name, display_name=a.display_name)
            for k, v in (
                    ('viaf', a.viaf),
                    ('lc', a.lc),
                    ('family_name', a.family_name),
                    ('display_name', a.display_name),
                    ('wikipedia_name', a.wikipedia_name)):
                if v:
                    author[k] = v
            authors.append(author)
        return authors

    def gutenberg_record(self, g, id):
        print "Starting work on %s" % g.title
        authors = self.author_list(g.authors)
        language=None
        if g.language:
            language=LanguageCodes.three_to_two.get(g.language, g.language)
        data = dict(id=id,
                    title=g.title,
                    subtitle=g.subtitle,
                    authors=authors,
                    language=language,
                    )
        data['subjects'] = self.subjects(g)

        if g.work:
            work = g.work
            data['adult_audience_confidence'] = self.adult_audience_confidence(work)
            data['fiction_confidence'] = self.fiction_confidence(work)
            data['popularity_rating'] = work.quality

        data['link_homepage'] = "http://gutenberg.org/ebooks/%s" % (
            g.primary_identifier.identifier)


        html_link = None
        epub_link = None

        r = g.best_open_access_link
        if r:
            data['link_epub'] = r.href
        else:
            data['link_epub'] = None

        if g.work:
            primary_identifier_ids = [
                x.primary_identifier.id for x in g.work.work_records]
        else:
            primary_identifier_ids = [g.primary_identifier.id]
        ids = WorkIdentifier.recursively_equivalent_identifier_ids(
            self._db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = WorkIdentifier.flatten_identifier_ids(ids)
        print "Got flat identifier IDs."
        resources = WorkIdentifier.resources_for_identifier_ids(
            self._db, flattened_data).options(lazyload('work_identifier'), lazyload('data_source')).all()
        print "Got %d resources." % len(resources)

        for r in resources:
            if not r.rel == Resource.OPEN_ACCESS_DOWNLOAD:
                continue
            if r.media_type.startswith('text/html'):
                if not 'link_html' in data and not 'zip' in r.href:
                    data['link_html'] = r.href
                    break

        cover_objs = []
        description_objs = []
        for r in resources:
            if r.rel == Resource.IMAGE and r.mirrored:
                o = dict(href=r.final_url,
                         _internal_id=r.id,
                         id=identifier_key(r.work_identifier),
                         source=r.data_source.name,
                         quality=r.quality,
                         image_height=r.image_height,
                         image_width=r.image_width)
                cover_objs.append(o)
            elif r.rel == Resource.DESCRIPTION:
                o = dict(
                    _internal_id=r.id,
                    id=identifier_key(r.work_identifier),
                    content=r.content,
                    quality=r.quality,
                    source=r.data_source.name)
                description_objs.append(o)

        data['links_cover'] = cover_objs
        data['descriptions'] = description_objs
        print "Done."
        return data
