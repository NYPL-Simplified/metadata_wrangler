"""Generate data sets for the Project Gutenberg Reanimator."""

from nose.tools import set_trace
import os
import json
from collections import defaultdict
from model import (
    DataSource,
    WorkRecord,
    WorkIdentifier,
)
from integration.oclc import (
    OCLCLinkedData,
)
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
            r = self.gutenberg_record(g, id)
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
            author = dict(name=a.name) 
            for k, v in (
                    ('viaf', a.viaf),
                    ('lc', a.lc)):
                if v:
                    author[k] = v
            authors.append(author)
        language=None
        if g.languages:
            language=LanguageCodes.three_to_two.get(g.languages[0], g.languages[0])
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
        for l in g.links.get('http://opds-spec.org/acquisition/open-access', []):
            if l['type'].startswith('text/html'):
                if not html_link or 'zip' in html_link:
                    html_link = l['href']
            elif l['type'].startswith('application/epub+zip'):
                if not epub_link or 'noimages' in epub_link:
                    epub_link = l['href']
        data['link_html'] = html_link
        data['link_epub'] = epub_link

        for link in g.links.get(WorkRecord.IMAGE, []):
            data['link_cover'] = link['href']
            break

        # Not available yet.
        data['links_internal_image'] = []

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
