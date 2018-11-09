# encoding: utf-8
import collections
import datetime
import json
import logging
import os
import re

import isbnlib
from collections import Counter
from pyld import jsonld
from nose.tools import set_trace

from sqlalchemy.orm.session import Session

from core.coverage import (
    IdentifierCoverageProvider,
)
from core.model import (
    get_one_or_create,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Subject,
)
from core.metadata_layer import (
    ContributorData,
    Metadata,
    LinkData,
    IdentifierData,
    SubjectData,
)
from core.util import (
    MetadataSimilarity,
    fast_query_count,
)

from coverage_utils import ResolveVIAFOnSuccessCoverageProvider
from viaf import VIAFClient


class ldq(object):

    @classmethod
    def for_type(self, g, search):
        check = [search, { "@id": search }]
        for node in g:
            if not isinstance(node, dict):
                continue
            for key in ('rdf:type', '@type'):
                node_type = node.get(key)
                if not node_type:
                    continue
                for c in check:
                    if node_type == c:
                        yield node
                        break
                    elif isinstance(node_type, list) and c in node_type:
                        yield node
                        break

    @classmethod
    def restrict_to_language(self, values, code_2):
        if isinstance(values, basestring) or isinstance(values, dict):
            values = [values]
        for v in values:
            if isinstance(v, basestring):
                yield v
            elif v and (not '@language' in v or v['@language'] == code_2):
                yield v

    @classmethod
    def values(self, vs):
        if isinstance(vs, basestring):
            yield vs
            return
        if isinstance(vs, dict) and '@value' in vs:
            yield vs['@value']
            return

        for v in vs:
            if isinstance(v, basestring):
                yield v
            elif '@value' in v:
                yield v['@value']


class OCLCLinkedData(object):

    BASE_URL = 'http://www.worldcat.org/%(type)s/%(id)s.jsonld'
    WORK_BASE_URL = 'http://experiment.worldcat.org/entity/work/data/%(id)s.jsonld'
    ISBN_BASE_URL = 'http://www.worldcat.org/isbn/%(id)s'
    URL_ID_RE = re.compile('http://www.worldcat.org/([a-z]+)/([0-9]+)')

    URI_WITH_OCLC_NUMBER = re.compile('^http://[^/]*worldcat.org/.*oclc/([0-9]+)$')
    URI_WITH_ISBN = re.compile('^http://[^/]*worldcat.org/.*isbn/([0-9X]+)$')
    URI_WITH_OCLC_WORK_ID = re.compile('^http://[^/]*worldcat.org/.*work/id/([0-9]+)$')

    EXTERNAL_PERSON_URI = re.compile('^http://experiment.worldcat.org/entity/person/data/([0-9]+)$')
    INTERNAL_PERSON_URI = re.compile('^http://experiment.worldcat.org/entity/work/data/[0-9]+#Person/[\w]+$')
    VIAF_ID = re.compile("^http://viaf.org/viaf/([0-9]+)/?$")

    CAN_HANDLE = set([Identifier.OCLC_WORK, Identifier.OCLC_NUMBER,
                      Identifier.ISBN])

    # We want to present metadata about a book independent of its
    # format, and metadata from audio books usually contains
    # information about the format.
    UNUSED_TYPES = set([
        'j.1:Audiobook',
        'j.1:Compact_Cassette',
        'j.1:Compact_Disc',
        'j.2:Audiobook',
        'j.2:Compact_Cassette',
        'j.2:Compact_Disc',
        'j.2:LP_record',
        'schema:AudioObject',
    ])

    # Publishers who are known to publish related but irrelevant
    # books, who basically republish Gutenberg books, who publish
    # books with generic-looking covers, or who are otherwise not good
    # sources of metadata.
    PUBLISHER_BLACKLIST = set([
        "General Books",
        "Cliffs Notes",
        "North Books",
        "Emereo",
        "Emereo Publishing",
        "Kessinger",
        "Kessinger Publishing",
        "Kessinger Pub.",
        "Recorded Books",
        ])

    # Barnes and Noble have boring book covers, but their ISBNs are likely
    # to have reviews associated with them.

    URI_TO_SUBJECT_TYPE = {
        re.compile("http://dewey.info/class/([^/]+).*") : Subject.DDC,
        re.compile("http://id.worldcat.org/fast/([^/]+)") : Subject.FAST,
        re.compile("http://id.loc.gov/authorities/subjects/(sh[^/]+)") : Subject.LCSH,
        re.compile("http://id.loc.gov/authorities/subjects/(jc[^/]+)") : Subject.LCSH,
    }

    ACCEPTABLE_TYPES = (
        'schema:Topic', 'schema:Place', 'schema:Person',
        'schema:Organization', 'schema:Event', 'schema:CreativeWork',
    )

    # These tags are useless for our purposes.
    POINTLESS_TAGS = set([
        'large type', 'large print', '(binding)', 'movable books',
        'electronic books', 'braille books', 'board books',
        'electronic resource', u'états-unis', 'etats-unis',
        'ebooks',
        ])

    # These tags indicate that the record as a whole is useless
    # for our purposes.
    #
    # However, they are not reliably assigned to records that are
    # actually useless, so we treat them the same as POINTLESS_TAGS.
    TAGS_FOR_UNUSABLE_RECORDS = set([
        'audiobook', 'audio book', 'sound recording', 'compact disc',
        'talking book', 'books on cd', 'audiocassettes', 'playaway',
        'vhs',
    ])

    FILTER_TAGS = POINTLESS_TAGS.union(TAGS_FOR_UNUSABLE_RECORDS)
    log = logging.getLogger("OCLC Linked Data Client")


    def __init__(self, _db):
        self._db = _db
        self.log = logging.getLogger("OCLC Linked Data")


    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

    def lookup(self, identifier_or_uri, processed_uris=set()):
        """Perform an OCLC Open Data lookup for the given identifier."""
        type = None
        identifier = None
        if isinstance(identifier_or_uri, basestring):
            # e.g. http://experiment.worldcat.org/oclc/1862341597.json
            match = self.URI_WITH_OCLC_NUMBER.search(identifier_or_uri)
            if match:
                type = Identifier.OCLC_NUMBER
                id = match.groups()[0]
                if not type or not id:
                    return None, None
                identifier, is_new = Identifier.for_foreign_id(
                    self._db, type, id)
        else:
            identifier = identifier_or_uri
            type = identifier.type
        if not type or not identifier:
            return None, None
        return self.lookup_by_identifier(identifier, processed_uris)

    def lookup_by_identifier(self, identifier, processed_uris=set()):
        """Turn an Identifier into a JSON-LD document."""
        if identifier.type == Identifier.OCLC_WORK:
            foreign_type = 'work'
            url = self.WORK_BASE_URL
        elif identifier.type == Identifier.OCLC_NUMBER:
            foreign_type = "oclc"
            url = self.BASE_URL

        url = url % dict(id=identifier.identifier, type=foreign_type)
        if url in processed_uris:
            self.log.debug("SKIPPING %s, already processed.", url)
            return None, True

        processed_uris.add(url)
        return self.get_jsonld(url)

    def get_jsonld(self, url):
        representation, cached = Representation.get(self._db, url)
        try:
            data = jsonld.load_document(url)
        except Exception as e:
            self.log.error("EXCEPTION on %s: %s", url, e, exc_info=e)
            return None, False

        if cached and not representation.content:
            representation, cached = Representation.get(
                self._db, url, max_age=0)

        if not representation.content:
            return None, False
        
        doc = {
            'contextUrl': None,
            'documentUrl': url,
            'document': representation.content.decode('utf8')
        }
        return doc, cached

    def oclc_number_for_isbn(self, isbn):
        """Turn an ISBN identifier into an OCLC Number identifier."""
        url = self.ISBN_BASE_URL % dict(id=isbn.identifier)
        representation, cached = Representation.get(
            self._db, url, Representation.http_get_no_redirect)
        if not representation.location:
            raise IOError(
                "Expected %s to redirect, but couldn't find location." % url
            )

        location = representation.location
        match = self.URI_WITH_OCLC_NUMBER.match(location)
        if not match:
            raise IOError(
                "OCLC redirected ISBN lookup, but I couldn't make sense of the destination, %s" % location)
        oclc_number = match.groups()[0]
        return Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, oclc_number)[0]

    def oclc_works_for_isbn(self, isbn, processed_uris=set()):
        """Yield every OCLC Work graph for the given ISBN."""
        # Find the OCLC Number for this ISBN.
        oclc_number = self.oclc_number_for_isbn(isbn)

        # Retrieve the OCLC Linked Data document for that OCLC Number.
        oclc_number_data, was_new = self.lookup_by_identifier(
            oclc_number, processed_uris)
        if not oclc_number_data:
            return

        # Look up every work referenced in that document and yield its data.
        graph = OCLCLinkedData.graph(oclc_number_data)
        works = OCLCLinkedData.extract_works(graph)
        for work_uri in works:
            m = self.URI_WITH_OCLC_WORK_ID.match(work_uri)
            if m:
                work_id = m.groups()[0]
                identifier, was_new = Identifier.for_foreign_id(
                    self._db, Identifier.OCLC_WORK, work_id)

                oclc_work_data, cached = self.lookup_by_identifier(
                    identifier, processed_uris)
                yield oclc_work_data

    @classmethod
    def creator_names(cls, graph, field_name='creator'):
        """Extract names and VIAF IDs for the creator(s) of the work described
        in `graph`.

        :param field_name: Try 'creator' first, then 'contributor' if
        that doesn't work.
        """
        names = []
        uris = []
        for book in cls.books(graph):
            values = book.get(field_name, [])
            for creator_uri in ldq.values(
                ldq.restrict_to_language(values, 'en')
            ):
                internal_results = cls.internal_lookup(graph, creator_uri)
                if internal_results:
                    for obj in internal_results:
                        for fieldname in ('name', 'schema:name'):
                            for name in ldq.values(obj.get(fieldname, [])):
                                names.append(name)
                else:
                    uris.append(creator_uri)
        return names, uris

    @classmethod
    def graph(cls, raw_data):
        if not raw_data or not raw_data['document']:
            return None
        try:
            document = json.loads(raw_data['document'])
        except ValueError, e:
            # We couldn't parse this JSON. It's _extremely_ rare from OCLC
            # but it does seem to happen.
            return dict()
        if not '@graph' in document:
            # Empty graph
            return dict()
        return document['@graph']

    @classmethod
    def books(cls, graph):
        if not graph:
            return
        for book in ldq.for_type(graph, "schema:Book"):
            yield book

    @classmethod
    def extract_workexamples(cls, graph):
        examples = []
        if not graph:
            return examples
        for book_graph in cls.books(graph):
            for k, repository in (
                    ('schema:workExample', examples),
                    ('workExample', examples),
            ):
                values = book_graph.get(k, [])
                repository.extend(ldq.values(values))
        return examples

    @classmethod
    def extract_works(cls, graph):
        works = []
        if not graph:
            return works
        for book_graph in cls.books(graph):
            for k, repository in (
                    ('schema:exampleOfWork', works),
                    ('exampleOfWork', works),
            ):
                values = book_graph.get(k, [])
                repository.extend(ldq.values(values))
        return works

    @classmethod
    def extract_contributor(cls, person_dict):
        """Extract a dict of args that can be used to create a Contributor or
        ContributorData object from an OCLC person entity graph.
        """
        def extract_names(name_list):
            # Sometimes OCLC sends back a list of names instead of
            # a single name. (This is really fun, of course!)
            names = list()
            for name in name_list:
                if isinstance(name, basestring):
                    names.append(name)
                if isinstance(name, dict):
                    more_names = list(ldq.restrict_to_language(name, 'en'))
                    [names.append(n) for n in extract_names(more_names)]
                if isinstance(name, list):
                    [names.append(n) for n in extract_names(name)]
            return names

        display_name = person_dict.get('name', None)
        if isinstance(display_name, list):
            display_name = cls._best_name_from_list(display_name)
        if not display_name:
            return None

        family_name = person_dict.get('familyName', None)
        if family_name and isinstance(family_name, list):
            family_name = cls._best_name_from_list(family_name)
        if ((display_name and family_name)
            and display_name.startswith(family_name+' ')):
            # Because _best_name_from_list trends toward the longest name,
            # sometimes the best name that we selected for the display name
            # is annoyingly formatted as LastName FirstName LastName. This
            # is a rudimentary fix.
            display_name = re.sub(family_name+' ', '', display_name)

        birth = person_dict.get('birthDate', None)
        death = person_dict.get('deathDate', None)

        extra = dict()
        if birth or death:
            def extract_year(date_string):
                if not date_string:
                    return None
                if isinstance(date_string, list):
                    date_string = date_string[0]
                if date_string.endswith(','):
                    date_string = date_string[:-1]
                if len(date_string)==4:
                    return date_string
                if re.match('\d{8}', date_string):
                    return date_string[0:4]
                dashed = date_string.split('-')
                slashed = date_string.split('/')
                for split_date in [dashed, slashed]:
                    if len(split_date) > 1:
                        year = [d for d in split_date if len(d)==4]
                        if year:
                            return year[0]

            if birth:
                birth = extract_year(birth)
                extra['birthDate'] = birth
            if death:
                death = extract_year(death)
                extra['deathDate'] = death
        return dict(
            display_name=display_name, family_name=family_name, extra=extra
        )

    @classmethod
    def _best_name_from_list(cls, name_list):
        """Selects the best contributor name data given a list of names"""

        names = list()
        for name_obj in name_list:
            if isinstance(name_obj, dict):
                if name_obj.get('@language', None) == 'en':
                    name_obj = name_obj.get('@value', None)
            if isinstance(name_obj, basestring):
                # Sometimes names in character-based languages are included,
                # without indication. They're being removed below by ensuring
                # some number of alphanumeric are present.
                if re.match('[A-z]+', name_obj):
                    names.append(name_obj)

        if not names:
            return None

        # Remove odd punctuation to try to create a higher-counted name option.
        names = [re.sub('[.,]', '', name) for name in names]
        if len(set(names)) == 1:
            # There's only one name, once you remove punctuation.
            return names[0]

        # Get all of the names with the highest count
        most_common = Counter(names).most_common()
        highest_count = most_common[0][1]
        most_common = [name for name, count in most_common if count==highest_count]
        if len(most_common)==1:
            return most_common[0]
        else:
            # Just pick the longest name to try to get the most data ¯\_(ツ)_/¯
            most_common.sort(cmp=lambda a,b: cmp(-len(a), -len(b)))
            return most_common[0]

    @classmethod
    def extract_useful_data(cls, subgraph, book):
        titles = []
        descriptions = []
        subjects = collections.defaultdict(list)
        publisher_uris = []
        creator_uris = []
        publication_dates = []
        example_uris = []

        no_value = (None, None, titles, descriptions, subjects, creator_uris,
                    publisher_uris, publication_dates, example_uris)

        if not book:
            return no_value

        id_uri = book['@id']
        m = cls.URL_ID_RE.match(id_uri)

        if not m:
            return no_value

        id_type, id = m.groups()
        if id_type == 'oclc':
            id_type = Identifier.OCLC_NUMBER
        elif id_type == 'work':
            # Kind of weird, but okay.
            id_type = Identifier.OCLC_WORK
        else:
            return no_value

        cls.log.info("Extracting %s: %s", id_type, id)
        for k, repository in (
                ('schema:description', descriptions),
                ('description', descriptions),
                ('schema:name', titles),
                ('name', titles),
                ('schema:datePublished', publication_dates),
                ('datePublished', publication_dates),
                ('workExample', example_uris),
                ('publisher', publisher_uris),
                ('creator', creator_uris),
                ('author', creator_uris)
        ):
            values = book.get(k, [])
            repository.extend(ldq.values(
                ldq.restrict_to_language(values, 'en')
            ))

        genres = book.get('genre', [])
        genres = list(ldq.values(ldq.restrict_to_language(genres, 'en')))
        genres = set(filter(None, [cls._fix_tag(tag) for tag in genres]))
        subjects[Subject.TAG] = [dict(id=genre) for genre in genres]

        for uri in book.get('about', []):
            if not isinstance(uri, basestring):
                continue

            subject_id = subject_type = subject_name = None

            # Grab FAST, DDC, and LCSH identifiers & types from their URIs.
            for r, canonical_subject_type in cls.URI_TO_SUBJECT_TYPE.items():
                m = r.match(uri)
                if m:
                    subject_id = m.groups()[0]
                    subject_type = canonical_subject_type
                    break

            # Try to pull information from an internal lookup.
            internal_lookup = cls.internal_lookup(subgraph, [uri])
            if not internal_lookup:
                # There's no extra data to be had. Take the subject and run.
                if subject_id and subject_type:
                    subjects[subject_type].append(dict(id=subject_id))
                continue
            [subject_data] = internal_lookup

            # Subject doesn't match known classification systems. Look
            # for an acceptable type.
            if not subject_type:
                type_objs = []
                for type_property in ('rdf:type', '@type'):
                    potential_types = subject_data.get(type_property, [])
                    if not isinstance(potential_types, list):
                        potential_types = [potential_types]
                    for potential_type in potential_types:
                        if isinstance(potential_type, dict):
                            type_objs.append(potential_type)
                        elif isinstance(potential_type, basestring):
                            type_objs.append({'@id': potential_type})
                for type_obj in type_objs:
                    type_id = type_obj['@id']
                    if type_id in cls.ACCEPTABLE_TYPES:
                        subject_type = type_id
                        break
                    elif type_id == 'schema:Intangible':
                        subject_type = Subject.TAG
                        break

            # Grab a human-readable name if possible.
            if subject_type:
                subject_names = None
                for name_property in ('name', 'schema:name'):
                    if name_property in subject_data:
                        subject_names = list(ldq.values(ldq.restrict_to_language(
                            subject_data[name_property], 'en'
                        )))
                    if subject_names:
                        subject_name = subject_names[0]
                        break

                # Set ids or names as appropriate & add to the list.
                if subject_id:
                    subjects[subject_type].append(
                        dict(id=subject_id, name=subject_name)
                    )
                elif subject_name:
                    subjects[subject_type].append(dict(id=subject_name))

        publishers = cls.internal_lookup(subgraph, publisher_uris)
        publisher_names = [i.get('schema:name') or i.get('name')
            for i in publishers
            if ('schema:name' in i or 'name' in i)]
        publisher_names = list(ldq.values(
            ldq.restrict_to_language(publisher_names, 'en')
        ))
        for n in publisher_names:
            if (n in cls.PUBLISHER_BLACKLIST
                or 'Audio' in n or 'Video' in n or 'Tape' in n
                or 'Comic' in n or 'Music' in n):
                # This book is from a publisher that will probably not
                # give us metadata we can use.
                return no_value

        return (id_type, id, titles, descriptions, subjects, creator_uris,
                publisher_names, publication_dates, example_uris)

    @classmethod
    def internal_lookup(cls, graph, uris):
        return [x for x in graph if x['@id'] in uris]

    @classmethod
    def _fix_tag(self, tag):
        if tag.endswith('.'):
            tag = tag[:-1]
        l = tag.lower()
        if any([x in l for x in self.FILTER_TAGS]):
            return None
        if l == 'cd' or l == 'cds':
            return None
        return tag

    def info_for(self, identifier):
        for data in self.graphs_for(identifier):
            subgraph = self.graph(data)
            for book in self.books(subgraph):
                info = self.book_info_to_metadata(subgraph, book)
                if info:
                    yield info

    def book_info_to_metadata(self, subgraph, book_info):
        """Filters raw book information to exclude irrelevant or unhelpful data.

        :returns: None if information is unhelpful; metadata object otherwise.
        """
        if not self._has_relevant_types(book_info):
            # This book is not available in any format we're
            # interested in from a metadata perspective.
            return None

        (oclc_id_type,
         oclc_id,
         titles,
         descriptions,
         subjects,
         creator_uris,
         publisher_names,
         publication_dates,
         example_uris) = self.extract_useful_data(subgraph, book_info)

        if not oclc_id_type or not oclc_id:
            return None

        self.log.info("Processing edition %s: %r", oclc_id, titles)
        metadata = Metadata(self.source)
        metadata.primary_identifier = IdentifierData(
            type=oclc_id_type, identifier=oclc_id
        )
        if titles:
            metadata.title = titles[0]
        for d in publication_dates:
            try:
                metadata.published = datetime.datetime.strptime(d[:4], "%Y")
            except Exception, e:
                pass

        for description in descriptions:
            # Create a description resource for every description.  When there's
            # more than one description for a given edition, only one of them is
            # actually a description. The others are tables of contents or some
            # other stuff we don't need. Unfortunately I can't think of an
            # automatic way to tell which is the good description.
            metadata.links.append(LinkData(
                Hyperlink.DESCRIPTION, media_type=Representation.TEXT_PLAIN,
                content=description,
            ))

        if 'Project Gutenberg' in publisher_names and not metadata.links:
            # Project Gutenberg texts don't have ISBNs, so if there's an
            # ISBN on there, it's probably wrong. Unless someone stuck a
            # description on there, there's no point in discussing
            # OCLC+LD's view of a Project Gutenberg work.
            return None
        if publisher_names:
            metadata.publisher = publisher_names[0]

        # Grab all the ISBNs.
        example_graphs = self.internal_lookup(subgraph, example_uris)
        for example in example_graphs:
            for isbn_name in 'schema:isbn', 'isbn':
                for isbn in ldq.values(example.get(isbn_name, [])):
                    if len(isbn) == 10:
                        isbn = isbnlib.to_isbn13(isbn)
                    elif len(isbn) != 13:
                        continue
                    if isbn:
                        metadata.identifiers.append(IdentifierData(
                            type = Identifier.ISBN, identifier = isbn
                        ))

        for subject_type, subjects_details in subjects.items():
            for subject_detail in subjects_details:
                if isinstance(subject_detail, dict):
                    subject_name = subject_detail.get('name')
                    subject_identifier = subject_detail.get('id')
                    metadata.subjects.append(SubjectData(
                        type=subject_type, identifier=subject_identifier,
                        name=subject_name,
                    ))
                else:
                    metadata.subjects.append(SubjectData(
                        type=subject_type, identifier=subject_detail
                    ))

        viafs = [self.VIAF_ID.search(uri) for uri in creator_uris]
        viafs = [viaf.groups()[0] for viaf in viafs if viaf is not None]
        for viaf in viafs:
            metadata.contributors.append(ContributorData(viaf=viaf))

        if creator_uris and not viafs:
            # We vastly prefer VIAF author information over OCLC.
            # We'll only extract OCLC author information if we have
            # _NO_ author information at all.
            contributors_data = []
            for uri in creator_uris:
                external = self.EXTERNAL_PERSON_URI.search(uri)
                if external:
                    contributors_data += self.get_contributors(uri)
                internal = self.INTERNAL_PERSON_URI.search(uri)
                if internal:
                    graphs = self.internal_lookup(subgraph, [uri])
                    contributor_data = None
                    for person_graph in graphs:
                        contributor_data = self.extract_contributor(person_graph)
                    if contributor_data:
                        contributors_data.append(contributor_data)

            for contributor_data in contributors_data:
                metadata.contributors.append(ContributorData(**contributor_data))

        if (not metadata.links and not metadata.identifiers and
            not metadata.subjects and not metadata.contributors):
            # Something interesting has to come out of this
            # work--something we couldn't get from another source--or
            # there's no point.
            return None

        return metadata

    def _has_relevant_types(self, book_info):
        type_objs = []
        for type_name in ('rdf:type', '@type'):
            these_type_objs = book_info.get(type_name, [])
            if not isinstance(these_type_objs, list):
                these_type_objs = [these_type_objs]
            for this_type_obj in these_type_objs:
                if isinstance(this_type_obj, dict):
                    type_objs.append(this_type_obj)
                elif isinstance(this_type_obj, basestring):
                    type_objs.append({"@id": this_type_obj})
        types = [i['@id'] for i in type_objs if
                 i['@id'] not in self.UNUSED_TYPES]
        return len(types) > 0

    def get_contributors(self, person_uri):
        """Creates ContributorData objects for OCLC Person entities"""
        document = self.get_jsonld(person_uri+'.jsonld')[0]
        if not document:
            return []

        graph = self.graph(document)
        if not graph:
            return []

        contributors_data = []
        for item in graph:
            if item['@id'] == person_uri:
                contributors_data = self.extract_contributor(item)
                if contributor:
                    contributors.append(contributor)
        return contributors

    def graphs_for(self, identifier):
        self.log.debug("BEGIN GRAPHS FOR %r", identifier)
        work_data = None

        if identifier.type in self.CAN_HANDLE:
            if identifier.type == Identifier.ISBN:
                work_data = list(self.oclc_works_for_isbn(identifier))
            elif identifier.type == Identifier.OCLC_WORK:
                work_data, cached = self.lookup(identifier)
            else:
                # Look up and yield a single edition.
                edition_data, cached = self.lookup(identifier)
                yield edition_data
                work_data = None

            if work_data:
                # We have one or more work graphs.
                if not isinstance(work_data, list):
                    work_data = [work_data]
                for data in work_data:
                    # Turn the work graph into a bunch of edition graphs.
                    if not data:
                        continue
                    self.log.debug(
                        "Handling work graph %s", data.get('documentUrl')
                    )
                    graph = self.graph(data)
                    examples = self.extract_workexamples(graph)
                    for uri in examples:
                        self.log.debug("Found example URI %s", uri)
                        data, cached = self.lookup(uri)
                        yield data

        else:
            # We got an identifier we can't handle. Turn it into a number
            # of identifiers we can handle.
            for i in identifier.equivalencies:
                if i.strength <= 0.7:
                    # TODO: This is a stopgap to make sure we don't
                    # turn low-strength equivalencies into
                    # high-strength ones.
                    continue
                if i.output.type in self.CAN_HANDLE:
                    for graph in self.graphs_for(i.output):
                        yield graph
        self.log.debug("END GRAPHS FOR %r", identifier)


class MockOCLCLinkedData(OCLCLinkedData):    
    def __init__(self, _db):
        super(MockOCLCLinkedData, self).__init__(_db)
        self._db = _db
        self.log = logging.getLogger("Mocked OCLC Linked Data")
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "tests", "files", "oclc")


    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


    def oclc_number_for_isbn(self, isbn):
        """Turn an ISBN identifier into an OCLC Number identifier."""

        # Let's pretend any id can be an oclc id.
        oclc_number = isbn.identifier
        oclc_identifier, made_new = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, oclc_number, autocreate=True)

        return oclc_identifier


    def oclc_works_for_isbn(self, isbn, processed_uris=set()):
        """Empty-yielding stub for: Yield every OCLC Work graph for the given ISBN."""

        # assume the calling test code has put a test file-derived graph into the queue
        # TODO: Code full functionality later.
        return None


    @classmethod
    def creator_names(cls, graph, field_name='creator'):
        """Empty-yielding stub for: Extract names and VIAF IDs for the creator(s) of the work described
        in `graph`.

        :param field_name: Try 'creator' first, then 'contributor' if
        that doesn't work.
        """
        # TODO: Code full functionality later.
        names = []
        uris = []
        return names, uris


class LinkedDataURLLister:
    """Gets all the work URLs, parses the graphs, and prints out a list of
    all the edition URLs.

    See scripts/generate_oclcld_url_list for why this is useful.
    """
    def __init__(self, _db, data_directory, output_file):
        self._db = _db
        self.data_directory = data_directory
        self.output_file = output_file
        self.oclc = OCLCLinkedData(self._db)

    def run(self):
        a = 0
        with open(self.output_file, "w") as output:
            for wi in self._db.query(Identifier).filter(
                    Identifier.type == Identifier.OCLC_WORK
                ).yield_per(100):
                data, cached = self.oclc.lookup(wi)
                graph = self.oclc.graph(data)
                examples = self.oclc.extract_workexamples(graph)
                for uri in examples:
                    uri = uri.replace("www.worldcat.org", "experiment.worldcat.org")
                    uri = uri + ".jsonld"
                    output.write(uri + ".jsonld")
                    output.write("\n")


class LinkedDataCoverageProvider(ResolveVIAFOnSuccessCoverageProvider):

    """Runs Editions obtained from OCLC Lookup through OCLC Linked Data.

    This (maybe) associates a edition with a (potentially) large
    number of ISBNs, which can be used as input into other services.
    """

    SERVICE_NAME = "OCLC Linked Data Coverage Provider"
    DEFAULT_BATCH_SIZE = 10

    DATA_SOURCE_NAME = DataSource.OCLC_LINKED_DATA
    INPUT_IDENTIFIER_TYPES = [
        Identifier.OCLC_WORK, Identifier.OCLC_NUMBER,
        Identifier.ISBN, Identifier.OVERDRIVE_ID
    ]
    
    def __init__(self, collection, *args, **kwargs):
        _db = Session.object_session(collection)
        api = kwargs.pop('api', None)
        if not api:
            api = OCLCLinkedData(_db)
        self.api = api

        viaf = kwargs.pop('viaf', None)
        if not viaf:
            viaf = VIAFClient(_db)
        self.viaf = viaf

        kwargs['registered_only'] = True
        super(LinkedDataCoverageProvider, self).__init__(
            collection, *args, **kwargs
        )
            
    def process_item(self, identifier):
        # Books are not looked up in OCLC Linked Data directly, since
        # there is no Collection that identifies a book by its OCLC Number.
        # However, when a book is looked up through OCLC Classify, some
        # OCLC Numbers may be associated with it, and _those_ numbers
        # can be run through OCLC Linked Data.
        try:
            new_info_counter = Counter()
            self.log.info("Processing identifier %r", identifier)
            metadatas = [m for m in self.api.info_for(identifier)]

            if identifier.type==Identifier.ISBN:
                # Currently info_for seeks the results of OCLC Work IDs only
                # This segment will get the metadata of any equivalent OCLC Numbers
                # as well.
                equivalents = Identifier.recursively_equivalent_identifier_ids(
                    self._db, [identifier.id]
                )
                oclc_numbers = self._db.query(Identifier).\
                    filter(Identifier.id.in_(equivalents)).\
                    filter(Identifier.type==Identifier.OCLC_NUMBER).all()
                for oclc_number in oclc_numbers:
                    more_metadata = [m for m in self.api.info_for(oclc_number)]
                    metadatas += more_metadata
                    metadatas = [m for m in metadatas if m]

            for metadata in metadatas:
                other_identifier, ignore = metadata.primary_identifier.load(self._db)
                oclc_editions = other_identifier.primarily_identifies

                # Keep track of the number of editions OCLC associates
                # with this identifier.
                other_identifier.add_measurement(
                    self.data_source, Measurement.PUBLISHED_EDITIONS,
                    len(oclc_editions)
                )

                # Clean up contributor information.
                self.apply_viaf_to_contributor_data(metadata)
                # Remove any empty ContributorData objects that may have
                # been created.
                metadata.contributors = filter(
                    lambda c: c.sort_name or c.display_name,
                    metadata.contributors
                )

                # When metadata is applied, it must be given a client that can
                # response to 'canonicalize_author_name'. Usually this is an
                # OPDSImporter that reaches out to the Metadata Wrangler, but
                # in the case of being _on_ the Metadata Wrangler...:
                from canonicalize import AuthorNameCanonicalizer
                metadata_client = AuthorNameCanonicalizer(
                    self._db, oclcld=self.api, viaf=self.viaf
                )

                num_new_isbns = self.new_isbns(metadata)
                new_info_counter['isbns'] += num_new_isbns
                if oclc_editions:
                    # There are existing OCLC editions. Apply any new information to them.
                    for edition in oclc_editions:
                        metadata, new_info_counter = self.apply_metadata_to_edition(
                            edition, metadata, metadata_client, new_info_counter
                        )
                else:
                    # Create a new OCLC edition to hold the information.
                    edition, ignore = get_one_or_create(
                        self._db, Edition, data_source=self.data_source,
                        primary_identifier=other_identifier
                    )
                    metadata, new_info_counter = self.apply_metadata_to_edition(
                        edition, metadata, metadata_client, new_info_counter
                    )
                    # Set the new OCLC edition's identifier equivalent to this
                    # identifier so we know they're related.
                    self.set_equivalence(identifier, metadata)

                self.log.info(
                    "Total: %(editions)d editions, %(isbns)d ISBNs, "\
                    "%(descriptions)d descriptions, %(subjects)d classifications.",
                    new_info_counter
                )
        except IOError as e:
            if ", but couldn't find location" in e.message:
                exception = "OCLC doesn't know about this ISBN: %r" % e
                transient = False
            else:
                exception = "OCLC raised an error: %r" % e
                transient = True
            return self.failure(identifier, exception, transient=transient)

        # Try to calculate or recalculate a work for ISBNs.
        #
        # We won't do this for other Identifier types because we don't want
        # to overwrite the high-quality metadata direct from the source.
        # With ISBNs, that higher-quality metadata is not available, so we
        # depend on OCLC for title and author information.
        if identifier.type == Identifier.ISBN:
            self.calculate_work_for_isbn(identifier)

        return identifier

    def apply_viaf_to_contributor_data(self, metadata):
        """Looks up VIAF information for contributors identified by OCLC

        This is particularly crucial for contributors identified solely
        by VIAF IDs (and no sort_name), as it raises errors later in the
        process.
        """
        for contributor_data in metadata.contributors:
            if contributor_data.viaf:
                viaf_contributor_data = self.viaf.lookup_by_viaf(
                    contributor_data.viaf,
                    working_sort_name=contributor_data.sort_name,
                    working_display_name=contributor_data.display_name
                )[0]
                if viaf_contributor_data:
                    viaf_contributor_data.apply(contributor_data)

    def apply_metadata_to_edition(self, edition, metadata, metadata_client, counter):
        """Applies metadata and increments counters"""
        metadata.apply(edition, collection=None, metadata_client=metadata_client)
        counter['editions'] += 1
        counter['descriptions'] += len(metadata.links)
        counter['subjects'] += len(metadata.subjects)

        return metadata, counter

    def new_isbns(self, metadata):
        """Returns the number of new isbns on a metadata object"""

        new_isbns = 0
        for identifier_data in metadata.identifiers:
            identifier, new = identifier_data.load(self._db)
            if new:
                new_isbns += 1
        return new_isbns

    def set_equivalence(self, identifier, metadata):
        """Identify the OCLC Number with the OCLC Work"""

        primary_editions = identifier.primarily_identifies
        if primary_editions:
            strength = 0
            for primary_edition in primary_editions:
                if metadata.title:
                    title_strength = MetadataSimilarity.title_similarity(
                        metadata.title, primary_edition.title
                    )
                else:
                    title_strength = 0
                edition_viafs = set(
                    [c.viaf for c in primary_edition.contributors if c.viaf]
                )
                metadata_viafs = set(
                    [c.viaf for c in metadata.contributors if c.viaf]
                )
                author_strength = MetadataSimilarity._proportion(
                    edition_viafs, metadata_viafs
                )
                edition_strength = (title_strength * 0.8) + (author_strength * 0.2)
                if edition_strength > strength:
                    strength = edition_strength
        else:
            strength = 1

        if strength > 0:
            primary_identifier, ignore = metadata.primary_identifier.load(
                self._db
            )
            identifier.equivalent_to(
                self.data_source, primary_identifier, strength
            )

    def calculate_work_for_isbn(self, identifier):
        """Uses recently-acquired OCLC metadata to try to create a work.

        This is of primary significance for ISBN identifiers, which will
        likely have no Work otherwise.
        """
        if identifier.type != Identifier.ISBN:
            return

        work = None
        license_pools = identifier.licensed_through
        if not license_pools:
            return

        self.generate_edition(identifier)

        license_pool = license_pools[0]
        license_pools = license_pools[1:]
        work, is_new = license_pool.calculate_work(exclude_search=True)
        if work and license_pools:
            for lp in license_pools:
                lp.work = work

    def generate_edition(self, identifier):
        """Utilizes an ISBN's equivalent identifiers (OCLC Number or Work IDs)
        to set an appropriate LicensePool presentation edition so a Work can
        later be created.
        """
        equivalent_ids = identifier.equivalent_identifier_ids()[identifier.id]

        # Get the editions of equivalent identifiers (OCLC Number or Work IDs)
        # to set as a presentation edition. These editions can be lower quality,
        # and it's important that they have a title.
        titled_equivalent_editions = self._db.query(Edition).\
            join(Edition.primary_identifier).\
            filter(Identifier.id.in_(equivalent_ids)).\
            filter(Edition.title!=None)

        # It's preferable that they have an author, too.
        authored_equivalent_editions = titled_equivalent_editions.filter(
            Edition.author!=None, Edition.author!=Edition.UNKNOWN_AUTHOR
        )

        if fast_query_count(authored_equivalent_editions):
            # Prioritize editions with both a title and an author if available.
            equivalent_editions = authored_equivalent_editions.all()
        else:
            equivalent_editions = titled_equivalent_editions.all()

        if equivalent_editions:
            # Set the presentation edition.
            pool = identifier.licensed_through[0]
            pool.set_presentation_edition(
                equivalent_editions=equivalent_editions
            )
