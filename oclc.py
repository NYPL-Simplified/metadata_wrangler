# encoding: utf-8
import collections
import datetime
import json
import logging
import re
import urllib

import isbnlib
from pyld import jsonld
from lxml import etree
from nose.tools import set_trace

from core.util.xmlparser import (
    XMLParser,
)
from core.coverage import (
    CoverageProvider,
    CoverageFailure,
)
from core.model import (
    Contributor,
    get_one_or_create,
    Hyperlink,
    Identifier,
    Edition,
    DataSource,
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
from core.util import MetadataSimilarity


class OCLC(object):
    """Repository for OCLC-related constants."""
    EDITION_COUNT = "OCLC.editionCount"
    HOLDING_COUNT = "OCLC.holdings"
    FORMAT = "OCLC.format"

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
            elif not '@language' in v or v['@language'] == code_2:
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

    def __init__(self, _db):
        self._db = _db
        self.source = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        self.log = logging.getLogger("OCLC Linked Data Client")

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
        representation, cached = Representation.get(self._db, url)
        try:
            data = jsonld.load_document(url)
        except Exception, e:
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
                "Expected %s to redirect, but couldn't find location." % url)
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
    def extract_useful_data(cls, subgraph, book):
        titles = []
        descriptions = []
        subjects = collections.defaultdict(set)
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
        ):
            values = book.get(k, [])
            repository.extend(ldq.values(
                ldq.restrict_to_language(values, 'en')
            ))

        genres = book.get('schema:genre', [])
        genres = [x for x in ldq.values(ldq.restrict_to_language(genres, 'en'))]
        subjects[Subject.TAG] = set(genres)

        internal_lookups = []
        for uri in book.get('about', []):
            if not isinstance(uri, basestring):
                continue
            for r, subject_type in cls.URI_TO_SUBJECT_TYPE.items():
                m = r.match(uri)
                if m:
                    subjects[subject_type].add(m.groups()[0])
                    break
            else:
                # Try an internal lookup.
                internal_lookups.append(uri)

        results = OCLCLinkedData.internal_lookup(subgraph, internal_lookups)
        for result in results:
            if 'schema:name' in result:
                name = result['schema:name']
            else:
                logging.getLogger("OCLC Linked Data Client").warn(
                    "WEIRD OCLC INTERNAL LOOKUP: %r", result)
                continue
            use_type = None
            type_objs = []
            for type_name in ('rdf:type', '@type'):
                these_type_objs = result.get(type_name, [])
                if not isinstance(these_type_objs, list):
                    these_type_objs = [these_type_objs]
                for this_type_obj in these_type_objs:
                    if isinstance(this_type_obj, dict):
                        type_objs.append(this_type_obj)
                    elif isinstance(this_type_obj, basestring):
                        type_objs.append({"@id": this_type_obj})

            for rdf_type in type_objs:
                if '@id' in rdf_type:
                    type_id = rdf_type['@id']
                else:
                    type_id = rdf_type
                if type_id in cls.ACCEPTABLE_TYPES:
                    use_type = type_id
                    break
                elif type_id == 'schema:Intangible':
                    use_type = Subject.TAG
                    break

            if use_type:
                for value in ldq.values(name):
                    subjects[use_type].add(value)

        publishers = cls.internal_lookup(subgraph, publisher_uris)
        publisher_names = [i['schema:name'] for i in publishers
                if 'schema:name' in i]
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
        self.log.info(
            "Processing edition %s: %r", book_info.get('oclc_id'),
            book_info.get('titles')
        )
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

        metadata = Metadata(self.source)
        metadata.primary_identifier, new = Identifier.for_foreign_id(
            self._db, oclc_id_type, oclc_id
        )
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

        # Consolidate subjects and apply a blacklist.
        fixed = [self._fix_tag(tag) for tag in subjects.get(Subject.TAG, [])]
        fixed_tags = [tag for tag in fixed if not None ]
        subjects[Subject.TAG] = fixed_tags

        for subject_type, subject_identifiers in subjects.items():
            for subject_identifier in subject_identifiers:
                metadata.subjects.append(SubjectData(
                    type=subject_type, identifier=subject_identifier
                ))

        for uri in creator_uris:
            viaf_uri = self.VIAF_ID.search(uri)
            if viaf_uri:
                viaf = viaf_uri.groups()[0]
                metadata.contributors.append(ContributorData(viaf=viaf))

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


class OCLCClassifyAPI(object):

    BASE_URL = 'http://classify.oclc.org/classify2/Classify?'

    NO_SUMMARY = '&summary=false'

    def __init__(self, _db):
        self._db = _db
        self.source = DataSource.lookup(self._db, DataSource.OCLC)

    def query_string(self, **kwargs):
        args = dict()
        for k, v in kwargs.items():
            if isinstance(v, unicode):
                v = v.encode("utf8")
            args[k] = v
        return urllib.urlencode(sorted(args.items()))

    def lookup_by(self, **kwargs):
        """Perform an OCLC Classify lookup."""
        query_string = self.query_string(**kwargs)
        url = self.BASE_URL + query_string
        representation, cached = Representation.get(self._db, url)
        return representation.content


class OCLCXMLParser(XMLParser):

    # OCLC in-representation 'status codes'
    SINGLE_WORK_SUMMARY_STATUS = 0
    SINGLE_WORK_DETAIL_STATUS = 2
    MULTI_WORK_STATUS = 4
    NO_INPUT_STATUS = 100
    INVALID_INPUT_STATUS = 101
    NOT_FOUND_STATUS = 102
    UNEXPECTED_ERROR_STATUS = 200

    INTS = set([OCLC.HOLDING_COUNT, OCLC.EDITION_COUNT])

    NAMESPACES = {'oclc' : 'http://classify.oclc.org'}

    LIST_TYPE = "works"
    log = logging.getLogger("OCLC XML Parser")

    @classmethod
    def parse(cls, _db, xml, **restrictions):
        """Turn XML data from the OCLC lookup service into a list of SWIDs
        (for a multi-work response) or a list of Edition
        objects (for a single-work response).
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        response = cls._xpath1(tree, "oclc:response")
        representation_type = int(response.get('code'))

        workset_record = None
        editions = []
        edition_records = []

        if representation_type == cls.UNEXPECTED_ERROR_STATUS:
            raise IOError("Unexpected error from OCLC API: %s" % xml)
        elif representation_type in (
                cls.NO_INPUT_STATUS, cls.INVALID_INPUT_STATUS):
            return representation_type, []
        elif representation_type == cls.SINGLE_WORK_SUMMARY_STATUS:
            raise IOError("Got single-work summary from OCLC despite requesting detail: %s" % xml)

        # The real action happens here.
        if representation_type == cls.SINGLE_WORK_DETAIL_STATUS:
            authors_tag = cls._xpath1(tree, "//oclc:authors")

            work_tag = cls._xpath1(tree, "//oclc:work")
            if work_tag is not None:
                author_string = work_tag.get('author')
                primary_author = cls.primary_author_from_author_string(_db, author_string)

            existing_authors = cls.extract_authors(
                _db, authors_tag, primary_author=primary_author)

            # The representation lists a single work, its authors, its editions,
            # plus summary classification information for the work.
            edition, ignore = cls.extract_edition(
                _db, work_tag, existing_authors, **restrictions)
            if edition:
                cls.log.info("EXTRACTED %r", edition)
            records = []
            if edition:
                records.append(edition)
            else:
                # The work record itself failed one of the
                # restrictions. None of its editions are likely to
                # succeed either.
                return representation_type, records

        elif representation_type == cls.MULTI_WORK_STATUS:
            # The representation lists a set of works that match the
            # search query.
            cls.log.debug("Extracting SWIDs from search results.")
            records = cls.extract_swids(_db, tree, **restrictions)
        elif representation_type == cls.NOT_FOUND_STATUS:
            # No problem; OCLC just doesn't have any data.
            records = []
        else:
            raise IOError("Unrecognized status code from OCLC API: %s (%s)" % (
                representation_type, xml))

        return representation_type, records

    @classmethod
    def extract_swids(cls, _db, tree, **restrictions):
        """Turn a multi-work response into a list of SWIDs."""

        swids = []
        for work_tag in cls._xpath(tree, "//oclc:work"):
            # We're not calling extract_basic_info because we care about
            # the info, we're calling it to make sure this work meets
            # the restriction. If this work meets the restriction,
            # we'll store its info when we look up the SWID.
            response = cls._extract_basic_info(
                _db, work_tag, **restrictions)
            if response:
                title, author_names, language = response
                # TODO: 'swid' is what it's called in older representations.
                # That code can be removed once we replace all representations.
                work_identifier = work_tag.get('wi') or work_tag.get('swid')
                cls.log.debug(
                    "WORK ID %s (%s, %r, %s)",
                    work_identifier, title, author_names, language
                )
                swids.append(work_identifier)
        return swids

    ROLES = re.compile("\[([^]]+)\]$")
    LIFESPAN = re.compile("([0-9]+)-([0-9]*)[.;]?$")

    @classmethod
    def extract_authors(cls, _db, authors_tag, primary_author=None):
        results = []
        if authors_tag is not None:
            for author_tag in cls._xpath(authors_tag, "//oclc:author"):
                lc = author_tag.get('lc', None)
                viaf = author_tag.get('viaf', None)
                contributor, roles, default_role_used = cls._parse_single_author(
                    _db, author_tag.text, lc=lc, viaf=viaf,
                    primary_author=primary_author)
                if contributor:
                    results.append(contributor)

        return results

    @classmethod
    def _contributor_match(cls, contributor, name, lc, viaf):
        return (
            contributor.name == name
            and (lc is None or contributor.lc == lc)
            and (viaf is None or contributor.viaf == viaf)
        )

    @classmethod
    def _parse_single_author(cls, _db, author,
                             lc=None, viaf=None,
                             existing_authors=[],
                             default_role=Contributor.AUTHOR_ROLE,
                             primary_author=None):
        default_role_used = False
        # First find roles if present
        # "Giles, Lionel, 1875-1958 [Writer of added commentary; Translator]"
        author = author.strip()
        m = cls.ROLES.search(author)
        if m:
            author = author[:m.start()].strip()
            role_string = m.groups()[0]
            roles = [x.strip() for x in role_string.split(";")]
        elif default_role:
            roles = [default_role]
            default_role_used = True
        else:
            roles = []

        # Author string now looks like
        # "Giles, Lionel, 1875-1958"
        m = cls.LIFESPAN.search(author)
        kwargs = dict()
        if m:
            author = author[:m.start()].strip()
            birth, death = m.groups()
            if birth:
                kwargs[Contributor.BIRTH_DATE] = birth
            if death:
                kwargs[Contributor.DEATH_DATE] = death

        # Author string now looks like
        # "Giles, Lionel,"
        if author.endswith(","):
            author = author[:-1]

        contributor = None
        if not author:
            # No name was given for the author.
            return None, roles, default_role_used

        if primary_author and author == primary_author.name:
            if Contributor.AUTHOR_ROLE in roles:
                roles.remove(Contributor.AUTHOR_ROLE)
            if Contributor.UNKNOWN_ROLE in roles:
                roles.remove(Contributor.UNKNOWN_ROLE)
            roles.insert(0, Contributor.PRIMARY_AUTHOR_ROLE)

        if existing_authors:
            # Calling Contributor.lookup will result in a database
            # hit, and looking up a contributor based on name may
            # result in multiple results (see below). We'll have no
            # way of distinguishing between those results. If
            # possible, it's much more reliable to look through
            # existing_authors (the authors derived from an entry's
            # <authors> tag).
            for x in existing_authors:
                if cls._contributor_match(x, author, lc, viaf):
                    contributor = x
                    break
            if contributor:
                was_new = False

        if not contributor:
            contributor, was_new = Contributor.lookup(
                _db, author, viaf, lc, extra=kwargs)
        if isinstance(contributor, list):
            # We asked for an author based solely on the name, which makes
            # Contributor.lookup() return a list.
            if len(contributor) == 1:
                # Fortunately, either the database knows about only
                # one author with that name, or it didn't know about
                # any authors with that name and it just created one,
                # so we can unambiguously use it.
                contributor = contributor[0]
            else:
                # Uh-oh. The database knows about multiple authors
                # with that name.  We have no basis for deciding which
                # author we mean. But we would prefer to identify with
                # an author who has a known LC or VIAF number.
                #
                # This should happen very rarely because of our check
                # against existing_authors above. But it will happen
                # for authors that have a work in Project Gutenberg.
                with_id = [x for x in contributor if x.lc is not None
                           or x.viaf is not None]
                if with_id:
                    contributor = with_id[0]
                else:
                    contributor = contributor[0]
        return contributor, roles, default_role_used

    @classmethod
    def primary_author_from_author_string(cls, _db, author_string):
        # If the first author mentioned in the author string
        # does not have an explicit role set, treat them as the primary
        # author.
        if not author_string:
            return None
        authors = author_string.split("|")
        if not authors:
            return None
        author, roles, default_role_used = cls._parse_single_author(
            _db, authors[0], default_role=Contributor.PRIMARY_AUTHOR_ROLE)
        if roles == [Contributor.PRIMARY_AUTHOR_ROLE]:
            return author
        return None

    @classmethod
    def parse_author_string(cls, _db, author_string, existing_authors=[],
                            primary_author=None):
        default_role = Contributor.PRIMARY_AUTHOR_ROLE
        authors = []
        if not author_string:
            return authors
        for author in author_string.split("|"):
            author, roles, default_role_used = cls._parse_single_author(
                _db, author, existing_authors=existing_authors,
                default_role=default_role,
                primary_author=primary_author)
            if roles:
                if Contributor.PRIMARY_AUTHOR_ROLE in roles:
                    # That was the primary author.  If we see someone
                    # with no explicit role after this point, they're
                    # just a regular author.
                    default_role = Contributor.AUTHOR_ROLE
                elif not default_role_used:
                    # We're dealing with someone whose role was
                    # explicitly specified. If we see someone with no
                    # explicit role after this point, it's probably
                    # because their role is so minor as to not be
                    # worth mentioning, not because it's so major that
                    # we can assume they're an author.
                    default_role = Contributor.UNKNOWN_ROLE
            roles = roles or [default_role]
            if author:
                authors.append((author, roles))
        return authors

    @classmethod
    def _extract_basic_info(cls, _db, tag, existing_authors=None,
                            **restrictions):
        """Extract information common to work tag and edition tag."""
        title = tag.get('title')
        author_string = tag.get('author')
        authors_and_roles = cls.parse_author_string(
            _db, author_string, existing_authors)
        if 'language' in tag.keys():
            language = tag.get('language')
        else:
            language = None

        if title and 'title' in restrictions:
            must_resemble_title = restrictions['title']
            threshold = restrictions.get('title_similarity', 0.25)
            similarity = MetadataSimilarity.title_similarity(
                must_resemble_title, title)
            if similarity < threshold:
                # The title of the book under consideration is not
                # similar enough to the given title.
                cls.log.debug(
                    "FAILURE TO RESEMBLE: %s vs %s (%.2f)",
                    title, must_resemble_title, similarity
                )
                return None

            # The semicolon is frequently used to separate multiple
            # works in an anthology. If there is no semicolon in the
            # original title, do not consider titles that contain
            # semicolons.
            if (not ' ; ' in must_resemble_title
                and ' ; ' in title and threshold > 0):
                cls.log.debug(
                    "SEMICOLON DISQUALIFICATION: %s", title
                )
                return None

        # Apply restrictions. If they're not met, return None.
        if 'language' in restrictions and language:
            # We know which language this record is for. Match it
            # against the language used in the Edition we're
            # matching against.
            restrict_to_language = set(restrictions['language'])
            if language != restrict_to_language:
                # This record is for a book in a different language
                cls.log.debug(
                    "WRONG LANGUAGE: %s", language
                )
                return None

        if 'authors' in restrictions:
            restrict_to_authors = restrictions['authors']
            if restrict_to_authors and isinstance(restrict_to_authors[0], Contributor):
                restrict_to_authors = [x.name for x in restrict_to_authors]
            primary_author = None

            for a, roles in authors_and_roles:
                if Contributor.PRIMARY_AUTHOR_ROLE in roles:
                    primary_author = a
                    break
            if (not primary_author
                or (primary_author not in restrict_to_authors
                    and primary_author.name not in restrict_to_authors)):
                    # None of the given authors showed up as the
                    # primary author of this book. They may have had
                    # some other role in it, or the book may be about
                    # them, or incorporate their work, but this book
                    # is not *by* them.
                return None

        author_names = ", ".join([x.name for x, y in authors_and_roles])

        return title, authors_and_roles, language

    UNUSED_MEDIA = set([
        "itemtype-intmm",
        "itemtype-msscr",
        "itemtype-artchap-artcl",
        "itemtype-jrnl",
        "itemtype-map",
        "itemtype-vis",
        "itemtype-jrnl-digital",
        "itemtype-image-2d",
        "itemtype-artchap-digital",
        "itemtype-intmm-digital",
        "itemtype-archv",
        "itemtype-msscr-digital",
        "itemtype-game",
        "itemtype-web-digital",
        "itemtype-map-digital",
    ])

    @classmethod
    def extract_edition(cls, _db, work_tag, existing_authors, **restrictions):
        """Create a new Edition object with information about a
        work (identified by OCLC Work ID).
        """
        # TODO: 'pswid' is what it's called in older representations.
        # That code can be removed once we replace all representations.
        oclc_work_id = unicode(work_tag.get('owi') or work_tag.get('pswid'))
        # if oclc_work_id:
        #     print " owi: %s" % oclc_work_id
        # else:
        #     print " No owi in %s" % etree.tostring(work_tag)


        if not oclc_work_id:
            raise ValueError("Work has no owi")

        item_type = work_tag.get("itemtype")
        if (item_type.startswith('itemtype-book')
            or item_type.startswith('itemtype-compfile')):
            medium = Edition.BOOK_MEDIUM
        elif item_type.startswith('itemtype-audiobook') or item_type.startswith('itemtype-music'):
            # Pretty much all Gutenberg texts, even the audio texts,
            # are based on a book, and the ones that aren't
            # (recordings of individual songs) probably aren't in OCLC
            # anyway. So we just want to get the books.
            medium = Edition.AUDIO_MEDIUM
            medium = None
        elif item_type.startswith('itemtype-video'):
            #medium = Edition.VIDEO_MEDIUM
            medium = None
        elif item_type in cls.UNUSED_MEDIA:
            medium = None
        else:
            medium = None

        # Only create Editions for books with a recognized medium
        if medium is None:
            return None, False

        result = cls._extract_basic_info(_db, work_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, language = result

        # Record some extra OCLC-specific information
        editions = work_tag.get('editions')
        holdings = work_tag.get('holdings')

        # Get an identifier for this work.
        identifier, ignore = Identifier.for_foreign_id(
            _db, Identifier.OCLC_WORK, oclc_work_id
        )

        data_source = DataSource.lookup(_db, DataSource.OCLC)
        identifier.add_measurement(data_source, Measurement.HOLDINGS, holdings)
        identifier.add_measurement(
            data_source, Measurement.PUBLISHED_EDITIONS, editions)


        # Create a Edition for source + identifier
        edition, new = get_one_or_create(
            _db, Edition,
            data_source=data_source,
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                language=language,
            )
        )

        # Get the most popular Dewey and LCC classification for this
        # work.
        for tag_name, subject_type in (
                ("ddc", Subject.DDC),
                ("lcc", Subject.LCC)):
            tag = cls._xpath1(
                work_tag,
                "//oclc:%s/oclc:mostPopular" % tag_name)
            if tag is not None:
                id = tag.get('nsfa') or tag.get('sfa')
                weight = int(tag.get('holdings'))
                identifier.classify(
                    data_source, subject_type, id, weight=weight)

        # Find FAST subjects for the work.
        for heading in cls._xpath(
                work_tag, "//oclc:fast//oclc:heading"):
            id = heading.get('ident')
            weight = int(heading.get('heldby'))
            value = heading.text
            identifier.classify(
                data_source, Subject.FAST, id, value, weight)

        # Associate the authors with the Edition.
        for contributor, roles in authors_and_roles:
            edition.add_contributor(contributor, roles)
        return edition, new

    @classmethod
    def extract_edition_record(cls, _db, edition_tag,
                               existing_authors,
                               **restrictions):
        """Create a new Edition object with information about an
        edition of a book (identified by OCLC Number).
        """
        oclc_number = unicode(edition_tag.get('oclc'))
        try:
            int(oclc_number)
        except ValueError, e:
            # This record does not have a valid OCLC number.
            return None, False

        # Fill in some basic information about this new record.
        result = cls._extract_basic_info(
            _db, edition_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, language = result

        # Add a couple extra bits of OCLC-specific information.
        extra = {
            OCLC.HOLDING_COUNT : edition_tag.get('holdings'),
            OCLC.FORMAT : edition_tag.get('itemtype'),
        }

        # Get an identifier for this edition.
        identifier, ignore = Identifier.for_foreign_id(
            _db, Identifier.OCLC_NUMBER, oclc_number
        )

        # Create a Edition for source + identifier
        data_source = DataSource.lookup(_db, DataSource.OCLC)
        edition_record, new = get_one_or_create(
            _db, Edition,
            data_source=data_source,
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                language=language,
                subjects=subjects,
                extra=extra,
            )
        )

        subjects = {}
        for subject_type, oclc_code in (
                (Subject.LCC, "050"),
                (Subject.DDC, "082")):
            classification = cls._xpath1(edition_tag,
                "oclc:classifications/oclc:class[@tag=%s]" % oclc_code)
            if classification is not None:
                value = classification.get("nsfa") or classification.get('sfa')
                identifier.classify(data_source, subject_type, value)

        # Associated each contributor with the new record.
        for author, roles in authors_and_roles:
            edition_record.add_contributor(author, roles)
        return edition_record, new


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


class LinkedDataCoverageProvider(CoverageProvider):

    """Runs Editions obtained from OCLC Lookup through OCLC Linked Data.

    This (maybe) associates a edition with a (potentially) large
    number of ISBNs, which can be used as input into other services.
    """

    def __init__(self, _db):
        self._db = _db
        self.api = OCLCLinkedData(self._db)
        self.output_source = DataSource.lookup(_db, DataSource.OCLC_LINKED_DATA)
        input_identifier_types = [
            Identifier.OCLC_WORK, Identifier.OCLC_NUMBER,
            Identifier.OVERDRIVE_ID, Identifier.THREEM_ID
        ]
        super(LinkedDataCoverageProvider, self).__init__(
            "OCLC Linked Data Coverage Provider", input_identifier_types,
            self.output_source, workset_size=10
        )

    def process_item(self, identifier):
        try:
            # Create counters.
            new_editions = new_isbns = new_descriptions = new_subjects = 0
            self.log.info("Processing identifier %r", identifier)

            for metadata in self.api.info_for(identifier):
                oclc_editions = metadata.primary_identifier.primarily_identifies
                num_new_isbns = self.new_isbns(metadata)
                if oclc_editions:
                    for edition in oclc_editions:
                        metadata.apply(edition)

                        # Increment counters for logging.
                        new_editions += 1
                        new_isbns += num_new_isbns
                        new_descriptions += len(metadata.links)
                        new_subjects += len(metadata.subjects)
                elif num_new_isbns:
                    edition = get_one_or_create(
                        self._db, Edition, data_source=self.output_source,
                        primary_identifier=metadata.primary_identifier
                    )
                    metadata.apply(edition)
                    self.set_equivalence(identifier, metadata)

                    # Increment counters for logging.
                    new_editions += 1
                    new_isbns += num_new_isbns
                    new_descriptions += len(metadata.links)
                    new_subjects += len(metadata.subjects)
                self.log.info(
                    "Total: %s editions, %s ISBNs, %s descriptions, %s classifications.",
                    new_editions, new_isbns, new_descriptions, new_subjects
                )
        except IOError as e:
            if ", but couldn't find location" in e.message:
                exception = "OCLC doesn't know about this ISBN: %r" % e
                return CoverageFailure(
                    self, identifier, exception, transient=False
                )
            exception = "OCLC raised an error: %r" % e
            return CoverageFailure(self, identifier, exception, transient=True)
        return identifier

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
            identifier.equivalent_to(
                self.output_source, metadata.primary_identifier, strength
            )
