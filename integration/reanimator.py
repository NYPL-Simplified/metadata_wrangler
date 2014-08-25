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
            r = self.gutenberg_record(g, id)

            # Now generate lists of equivalencies.
            work_obj = g.work
            if work_obj:
                for wr in work_obj.work_records:
                    if wr.primary_identifier.type == WorkIdentifier.OCLC_WORK:
                        try:
                            work, editions = self.oclc_work_record(wr)
                        except Exception, e:
                            print "Could not load OCLC work record for %s" % wr.primary_identifier
                            work = None
                            editions = []
                        if work:
                            json.dump(work, oclc_work_output)
                            oclc_work_output.write("\n")

                            similarity = work_obj.similarity_to(wr)
                            self.add_equivalency(
                                equivalency_output, id, work['id'], similarity)
                                         
                        for edition in editions:
                            json.dump(edition, oclc_edition_output)
                            oclc_edition_output.write("\n")                   
                            self.add_equivalency(
                                equivalency_output, edition['id'], work['id'], 0.7)

            json.dump(r, gutenberg_output)
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

    def gutenberg_record(self, g, id):
        authors = []
        print "Starting work on %s" % g.title
        for a in g.authors:
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

        data['link_epub'] = g.best_open_access_link

        primary_identifier_ids = [
            x.primary_identifier.id for x in g.work.work_records]
        ids = WorkIdentifier.recursively_equivalent_identifier_ids(
            self._db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = WorkIdentifier.flatten_identifier_ids(ids)
        print "Got flat identifier IDs."
        resources = WorkIdentifier.resources_for_identifier_ids(
            self._db, flattened_data).options(lazyload('work_identifier'), lazyload('data_source')).all()
        print "Got resources."

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
                         id=r.id,
                         identifier=[r.work_identifier.type,
                                     r.work_identifier.identifier],
                         source=r.data_source.name,
                         quality=r.quality,
                         image_height=r.image_height,
                         image_width=r.image_width)
                cover_objs.append(o)
            elif r.rel == Resource.DESCRIPTION:
                o = dict(
                    id=r.id,
                    identifier=[r.work_identifier.type, r.work_identifier.identifier],
                    content=r.content,
                    quality=r.quality,
                    source=r.data_source.name)
                description_objs.append(o)

        data['links_cover'] = cover_objs
        data['descriptions'] = description_objs
        print "Done."
        return data

    BAD_TYPES = ('schema:AudioObject', 'j.2:Audiobook',
                 'j.2:Compact_Disc', 'j.2:LP_record',
                 'j.1:Audiobook', 'j.2:Compact_Cassette',
                 'j.1:Compact_Cassette', 'j.1:Compact_Disc')

    def oclc_work_record(self, wr):

        identifier = wr.primary_identifier
        work_record = dict(id="owi-%s" % identifier.identifier)

        graph, cached = self.oclc_ld.lookup(identifier)
        graph = json.loads(graph['document'])['@graph']
        #titles, descriptions, authors, subjects = self.oclc_ld.extract_useful_data(graph)
        #work_record['description'] = descriptions

        edition_records = []
        examples = set(self.oclc_ld.extract_workexamples(graph))
        for uri in examples:

            data, cached = self.oclc_ld.lookup(uri)
            subgraph = json.loads(data['document'])['@graph']
            isbns = set()

            for item in subgraph:
                if 'schema:isbn' in item:
                    isbns = isbns.union(item.get('schema:isbn'))

            for book in self.oclc_ld.books(subgraph):
                description = book.get('schema:description')
                if isinstance(description, dict):
                    description = description['@value']

                if not description and not isbns:
                    # No reason to consider this edition.
                    continue

                publisher_url = book.get('publisher', None)
                p = [x for x in subgraph if x['@id'] == publisher_url]
                if p:
                    publisher = p[0]['schema:name']
                else:
                    publisher = None
                # examples = set(ldq.values(book.get('workExample', [])))
                published = book.get('schema:datePublished', None)
                good_type = True
                types = book.get('rdf:type', [])
                if isinstance(types, dict):
                    types = [types]
                for i in types:
                    if i['@id'] in self.BAD_TYPES:
                        good_type = False
                        break
                    elif i['@id'] != 'schema:Book':
                        print i['@id']
                    if not good_type:
                        break
                if not good_type:
                    continue

                title = book.get('schema:name')
                if isinstance(title, dict):
                    title = title['@value']

                language = book.get('schema:inLanguage', None)
                if isinstance(language, list):
                    language = language[0]

                edition_record = dict(
                    id = "OCLC-%s" % uri[len('http://www.worldcat.org/oclc/'):],
                    language = language,
                    datePublished=published,
                    isbns=list(isbns))
                for k, v in (('title', title),
                             ('description', description),
                             ('publisher', publisher)):
                    if v:
                        edition_record[k] = v
                edition_records.append(edition_record)

        return work_record, edition_records
