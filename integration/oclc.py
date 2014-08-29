import collections
import datetime
import json
import md5
import os
import pprint
import re
import requests
import time
import urllib

import isbnlib
from pyld import jsonld
from lxml import etree
from nose.tools import set_trace

from integration import (
    XMLParser,
)
from monitor import Monitor
from integration import FilesystemCache
from model import (
    Contributor,
    CoverageProvider,
    get_one,
    get_one_or_create,
    WorkIdentifier,
    WorkRecord,
    DataSource,
    Resource,
    Subject,
)
from util import MetadataSimilarity


class OCLC(object):
    """Repository for OCLC-related constants."""
    EDITION_COUNT = "OCLC.editionCount"
    HOLDING_COUNT = "OCLC.holdings"
    FORMAT = "OCLC.format"

class ldq(object):
    
    @classmethod
    def for_type(self, g, search):
        check = { "@id": search }
        for node in g:
            if not isinstance(node, dict):
                continue
            node_type = node.get('rdf:type')
            if not node_type:
                continue
            if node_type == check:
                yield node
            elif isinstance(node_type, list) and check in node_type:
                yield node

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
    URL_ID_RE = re.compile('http://www.worldcat.org/([a-z]+)/([0-9]+)')

    def __init__(self, data_directory):
        self.cache_directory = os.path.join(
            data_directory, DataSource.OCLC_LINKED_DATA, "cache")
        self.cache = FilesystemCache(self.cache_directory)

    def cache_key(self, id, type):
        return os.path.join(type, "%s.jsonld" % id)

    def request(self, url):
        """Make a request to OCLC Linked Data."""
        response = requests.get(url)
        content = response.content
        if response.status_code == 404:
            return ''
        elif response.status_code == 500:
            return None
        elif response.status_code != 200:
            raise IOError("OCLC Linked Data returned status code %s: %s" % (response.status_code, response.content))
        return content

    URI_WITH_OCLC_NUMBER = re.compile('http://www.worldcat.org/oclc/([0-9]+)')
    def lookup(self, work_identifier):
        """Perform an OCLC Open Data lookup for the given identifier."""

        type = None
        identifier = None
        if isinstance(work_identifier, basestring):
            match = self.URI_WITH_OCLC_NUMBER.search(work_identifier)
            if match:
                type = WorkIdentifier.OCLC_NUMBER
                identifier = match.groups()[0]
        else:
            type = work_identifier.type
            identifier = work_identifier.identifier
        if not type or not identifier:
            return None
        return self.lookup_by_identifier(type, identifier)

    def lookup_by_identifier(self, type, identifier):
        if type == WorkIdentifier.OCLC_WORK:
            foreign_type = 'work'
            url = self.WORK_BASE_URL
        elif type == WorkIdentifier.OCLC_NUMBER:
            foreign_type = "oclc"
            url = self.BASE_URL

        cache_key = self.cache_key(identifier, foreign_type)
        cached = False
        if self.cache.exists(cache_key):
            cached = True
        else:
            url = url % dict(id=identifier, type=foreign_type)
            print "%s => %s" % (url, self.cache._filename(cache_key))
            raw = self.request(url) or ''
            self.cache.store(cache_key, raw)
        f = self.cache._filename(cache_key)
        url = "file://" + f
        data = jsonld.load_document(url)
        return data, cached

    @classmethod
    def graph(cls, raw_data):
        if not raw_data or not raw_data['document']:
            return None
        document = json.loads(raw_data['document'])
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

    URI_TO_SUBJECT_TYPE = {
        re.compile("http://dewey.info/class/([^/]+).*") : Subject.DDC,
        re.compile("http://id.worldcat.org/fast/([^/]+)") : Subject.FAST,
        re.compile("http://id.loc.gov/authorities/subjects/sh([^/]+)") : Subject.LCSH,
    }

    ACCEPTABLE_TYPES = 'schema:Topic', 'schema:Place', 'schema:Person', 'schema:Organization', 'schema:Event'

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
            id_type = WorkIdentifier.OCLC_NUMBER
        elif id_type == 'work':
            # Kind of weird, but okay.
            id_type = WorkIdentifier.OCLC_WORK
        else:
            print "EXPECTED OCLC ID, got %s" % id_type
            return no_value

        for k, repository in (
                ('schema:description', descriptions),
                ('schema:name', titles),
                ('schema:datePublished', publication_dates),
                ('workExample', example_uris),
                ('publisher', publisher_uris),
                ('creator', creator_uris),
        ):
            values = book.get(k, [])
            repository.extend(ldq.values(
                ldq.restrict_to_language(values, 'en')))

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
                print "WEIRD INTERNAL LOOKUP: %r" % result
                continue
            use_type = None
            if 'rdf:type' in result:
                types = result.get('rdf:type', [])
                if isinstance(types, dict):
                    types = [types]
                for rdf_type in types:
                    if '@id' in rdf_type:
                        type_id = rdf_type['@id']
                    if type_id in cls.ACCEPTABLE_TYPES:
                        use_type = type_id
                        break
                    elif type_id == 'schema:Intangible':
                        use_type = Subject.TAG
                        break
                    print type_id, result
                    
            if use_type:
                for value in ldq.values(name):
                    subjects[use_type].add(value)

        return (id_type, id, titles, descriptions, subjects, creator_uris,
                publisher_uris, publication_dates, example_uris)

    @classmethod
    def internal_lookup(cls, graph, uris):
        return [x for x in graph if x['@id'] in uris]


oclc_linked_data = None
if 'DATA_DIRECTORY' in os.environ:
    oclc_linked_data = OCLCLinkedData(os.environ['DATA_DIRECTORY'])


class XIDAPI(object):

    OCLC_ID_TYPE = "oclcnum"
    ISBN_ID_TYPE = "isbn"

    BASE_URL = 'http://xisbn.worldcat.org/webservices/xid/%(type)s/%(id)s'

    ARGUMENTS = '?method=getEditions&format=json&fl=*'

    def __init__(self, data_directory):
        self.cache_directory = os.path.join(
            data_directory, DataSource.XID, "cache")
        self.cache = FilesystemCache(self.cache_directory)

    def cache_key(self, id, type):
        return "%s-%s" % (type, id)

    def request(self, url):
        """Make a request to the xID API."""
        response = requests.get(url)
        content = response.content
        if response.status_code == 404:
            return None
        elif response.status_code != 200:
            raise IOError("xID API returned status code %s: %s" % (response.status_code, response.content))
        return content

    def get_editions(self, id, type=None):
        """Perform an OCLC lookup."""
        type = type or self.OCLC_ID_TYPE
        cache_key = self.cache_key(id, type)
        raw = None
        cached = False
        if self.cache.exists(cache_key):
            # Don't go over the wire. Get the raw XML from cache
            # and process it fresh.
            raw = self.cache.open(cache_key).read()
            cached = True
        if not raw:
            url = self.BASE_URL % dict(id=id, type=type)
            url += self.ARGUMENTS
            print "Requesting %s" % url
            raw = self.request(url) or ''
            print " Retrieved over the net."
            self.cache.store(cache_key, raw)
        return raw, cached

class OCLCClassifyAPI(object):

    BASE_URL = 'http://classify.oclc.org/classify2/Classify?'

    NO_SUMMARY = '&summary=false'

    def __init__(self, data_directory):
        self.cache_directory = os.path.join(
            data_directory, DataSource.OCLC, "cache")
        self.cache = FilesystemCache(self.cache_directory)
        self.last_access = None

    def query_string(self, **kwargs):
        args = dict()
        for k, v in kwargs.items():
            if isinstance(v, unicode):
                v = v.encode("utf8")
            args[k] = v
        return urllib.urlencode(sorted(args.items()))
        
    def cache_key(self, **kwargs):
        qs = self.query_string(**kwargs)
        if len(qs) > 18: # Length of "isbn=[isbn13]"
            return md5.md5(qs).hexdigest()
        return qs

    def request(self, url):
        """Make a request to the OCLC classification API."""
        response = requests.get(url)
        content = response.content
        if response.status_code == 404:
            return None
        elif response.status_code != 200:
            raise IOError("OCLC API returned status code %s: %s" % (response.status_code, response.content))
        return content

    def lookup_by(self, **kwargs):
        """Perform an OCLC lookup."""
        query_string = self.query_string(**kwargs)
        cache_key = self.cache_key(**kwargs)
        #print " Query string: %s" % query_string
        raw = None
        if self.cache.exists(cache_key):
            # Don't go over the wire. Get the raw XML from cache
            # and process it fresh.
            raw = self.cache.open(cache_key).read()
        if not raw:
            url = self.BASE_URL + query_string + self.NO_SUMMARY
            #print " URL: %s" % url
            raw = self.request(url) or ''
            #print " Retrieved over the net."
            self.cache.store(cache_key, raw)

        return raw

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

    @classmethod
    def parse(cls, _db, xml, **restrictions):
        """Turn XML data from the OCLC lookup service into a list of SWIDs
        (for a multi-work response) or a list of WorkRecord
        objects (for a single-work response).
        """
        tree = etree.fromstring(xml, parser=etree.XMLParser(recover=True))
        response = cls._xpath1(tree, "oclc:response")
        representation_type = int(response.get('code'))

        workset_record = None
        work_records = []
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
            existing_authors = cls.extract_authors(_db, authors_tag)

            # The representation lists a single work, its authors, its editions,
            # plus summary classification information for the work.
            work_tag = cls._xpath1(tree, "//oclc:work")
            work_record, ignore = cls.extract_work_record(
                _db, work_tag, existing_authors, **restrictions)
            records = []
            if work_record:
                records.append(work_record)
            else:
                # The work record itself failed one of the
                # restrictions. None of its editions are likely to
                # succeed either.
                return representation_type, records

            # data_source = DataSource.lookup(_db, DataSource.OCLC)
            # for edition_tag in cls._xpath(work_tag, '//oclc:edition'):
            #     edition_record, ignore = cls.extract_edition_record(
            #         _db, edition_tag, existing_authors, **restrictions)
            #     if not edition_record:
            #         # This edition did not become a WorkRecord because it
            #         # didn't meet one of the restrictions.
            #         continue
            #     records.append(edition_record)
            #     # Identify the edition with the work based on its
            #     # primary identifier.
            #     work_record.primary_identifier.equivalent_to(
            #         data_source, edition_record.primary_identifier)
            #     edition_record.primary_identifier.equivalent_to(
            #         data_source, work_record.primary_identifier)
        elif representation_type == cls.MULTI_WORK_STATUS:
            # The representation lists a set of works that match the
            # search query.
            #print "Extracting SWIDs from search results."
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
                swids.append(work_tag.get('swid'))
        return swids

    ROLES = re.compile("\[([^]]+)\]$")
    LIFESPAN = re.compile("([0-9]+)-([0-9]*)[.;]?$")

    @classmethod
    def extract_authors(cls, _db, authors_tag):
        results = []
        if authors_tag is not None:
            for author_tag in cls._xpath(authors_tag, "//oclc:author"):
                lc = author_tag.get('lc', None)
                viaf = author_tag.get('viaf', None)
                contributor, roles = cls._parse_single_author(
                    _db, author_tag.text, lc=lc, viaf=viaf)
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
                             default_role=Contributor.AUTHOR_ROLE):
        # First find roles if present
        # "Giles, Lionel, 1875-1958 [Writer of added commentary; Translator]"
        author = author.strip()
        m = cls.ROLES.search(author)
        if m:
            author = author[:m.start()].strip()
            role_string = m.groups()[0]
            roles = [x.strip() for x in role_string.split(";")]
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
            return None, roles
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
        return contributor, roles

    @classmethod
    def parse_author_string(cls, _db, author_string, existing_authors=[]):
        default_role = Contributor.AUTHOR_ROLE
        authors = []
        if not author_string:
            return authors
        for author in author_string.split("|"):            
            author, roles = cls._parse_single_author(
                _db, author, existing_authors=existing_authors,
                default_role=default_role)
            if roles:
                # If we see someone with no explicit role after this
                # point, it's probably because their role is so minor
                # as to not be worth mentioning, not because it's so
                # major that we can assume they're an author.
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
                #print " FAILURE TO RESEMBLE: %s vs %s (%.2f)" % (title, must_resemble_title, similarity)
                return None

            # The semicolon is frequently used to separate multiple
            # works in an anthology. If there is no semicolon in the
            # original title, do not consider titles that contain
            # semicolons.
            if (not ' ; ' in must_resemble_title
                and ' ; ' in title and threshold > 0):
                #print "SEMICOLON DISQUALIFICATION: %s" % title
                return None

        # Apply restrictions. If they're not met, return None.
        if 'language' in restrictions and language:
            # We know which language this record is for. Match it
            # against the language used in the WorkRecord we're
            # matching against.
            restrict_to_language = set(restrictions['language'])
            if language != restrict_to_language:
                # This record is for a book in a different language
                return None

        if 'authors' in restrictions:
            restrict_to_authors = restrictions['authors']
            authors_per_se = set([
                a.name for a, roles in authors_and_roles if Contributor.AUTHOR_ROLE in roles
            ])
            for restrict_to_author in restrict_to_authors:
                if not restrict_to_author.name in authors_per_se:
                    # The given author did not show up as one of the
                    # per se 'authors' of this book. They may have had
                    # some other role in it, or the book may be about
                    # them, but this book is not *by* them.
                    return None

        author_names = ", ".join([x.name for x, y in authors_and_roles])

        print u" SUCCESS %s, %r, %s" % (title, author_names, language)
        return title, authors_and_roles, language

    @classmethod
    def extract_work_record(cls, _db, work_tag, existing_authors, **restrictions):
        """Create a new WorkRecord object with information about a
        work (identified by OCLC Work ID).
        """
        oclc_work_id = unicode(work_tag.get('pswid'))
        # if oclc_work_id:
        #     print " owi: %s" % oclc_work_id
        # else:
        #     print " No owi in %s" % etree.tostring(work_tag)


        try:
            int(oclc_work_id)
        except ValueError, e:
            # This record does not have a valid OCLC Work ID.
            return None, False

        result = cls._extract_basic_info(_db, work_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, language = result


        # Record some extra OCLC-specific information
        extra = {
            OCLC.EDITION_COUNT : work_tag.get('editions'),
            OCLC.HOLDING_COUNT : work_tag.get('holdings'),
            OCLC.FORMAT : work_tag.get('itemtype'),
        }
        
        # Get an identifier for this work.
        identifier, ignore = WorkIdentifier.for_foreign_id(
            _db, WorkIdentifier.OCLC_WORK, oclc_work_id
        )

        # Create a WorkRecord for source + identifier
        data_source=DataSource.lookup(_db, DataSource.OCLC)
        work_record, new = get_one_or_create(
            _db, WorkRecord,
            data_source=data_source,
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                language=language,
                extra=extra,
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

        # Associate the authors with the WorkRecord.
        for contributor, roles in authors_and_roles:
            work_record.add_contributor(contributor, roles)
        return work_record, new

    @classmethod
    def extract_edition_record(cls, _db, edition_tag,
                               existing_authors,
                               **restrictions):
        """Create a new WorkRecord object with information about an
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
        identifier, ignore = WorkIdentifier.for_foreign_id(
            _db, WorkIdentifier.OCLC_NUMBER, oclc_number
        )

        # Create a WorkRecord for source + identifier
        data_source = DataSource.lookup(_db, DataSource.OCLC)
        edition_record, new = get_one_or_create(
            _db, WorkRecord,
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
    def __init__(self, db, data_directory, output_file):
        self.db = db
        self.data_directory = data_directory
        self.output_file = output_file
        self.oclc = OCLCLinkedData(self.data_directory)        

    def run(self):
        a = 0
        with open(self.output_file, "w") as output:
            for wi in self.db.query(WorkIdentifier).filter(
                    WorkIdentifier.type==WorkIdentifier.OCLC_WORK).yield_per(100):
                data, cached = self.oclc.lookup(wi)
                graph = self.oclc.graph(data)
                examples = self.oclc.extract_workexamples(graph)
                for uri in examples:
                    uri = uri.replace("www.worldcat.org", "experiment.worldcat.org")
                    uri = uri + ".jsonld"
                    output.write(uri + ".jsonld")
                    output.write("\n")

class LinkedDataCoverageProvider(CoverageProvider):

    """Runs WorkRecords obtained from OCLC Lookup through OCLC Linked Data.
    
    This (maybe) associates a workrecord with a (potentially) large
    number of ISBNs, which can be used as input into other services.
    """

    SERVICE_NAME = "OCLC Linked Data from OCLC Classify"

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

    def __init__(self, db, data_directory):
        self.oclc = OCLCLinkedData(data_directory)
        self.db = db
        oclc_classify = DataSource.lookup(db, DataSource.OCLC)
        self.oclc_linked_data = DataSource.lookup(db, DataSource.OCLC_LINKED_DATA)
        self.coverage_provider = CoverageProvider(
            "OCLC-LD lookup", oclc_classify, self.oclc_linked_data)
        super(LinkedDataCoverageProvider, self).__init__(
            self.SERVICE_NAME, oclc_classify, self.oclc_linked_data,
            workset_size=10)

    def process_work_record(self, wr):
        try:
            oclc_work = wr.primary_identifier
            new_records = 0
            new_isbns = 0
            new_descriptions = 0
            print u"%s (%s)" % (wr.title, repr(oclc_work).decode("utf8"))
            for edition in self.info_for(oclc_work):
                workrecord, isbns, descriptions, subjects = self.process_edition(oclc_work, edition)
                if workrecord:
                    new_records += 1
                    print "", workrecord.publisher, len(isbns), len(descriptions)
                new_isbns += len(isbns)
                new_descriptions += len(descriptions)

            print "Total: %s ISBNs, %s descriptions." % (
                new_isbns, new_descriptions)
        except IOError, e:
            return False
        return True

    def process_edition(self, oclc_work, edition):
        publisher = None
        if edition['publishers']:
            publisher = edition['publishers'][0]

        # We should never need this title, but it's helpful
        # for documenting what's going on.
        title = None
        if edition['titles']:
            title = edition['titles'][0]

        # Try to find a publication year.
        publication_date = None
        for d in edition['publication_dates']:
            d = d[:4]
            try:
                publication_date = datetime.datetime.strptime(
                    d[:4], "%Y")
            except Exception, e:
                pass

        oclc_number, new = WorkIdentifier.for_foreign_id(
            self.db, edition['oclc_id_type'],
            edition['oclc_id'])

        # Associate classifications with the OCLC number.
        classifications = []
        for subject_type, subject_ids in edition['subjects'].items():
            for subject_id in subject_ids:
                new_classes = oclc_number.classify(
                    self.oclc_linked_data, subject_type, subject_id)
                classifications.extend(new_classes)

        # Create new ISBNs associated with the OCLC
        # number. This will help us get metadata from other
        # sources that use ISBN as input.
        new_isbns_for_this_oclc_number = []
        for isbn in edition['isbns']:
            isbn_identifier, new = WorkIdentifier.for_foreign_id(
                self.db, WorkIdentifier.ISBN, isbn)
            if new:
                new_isbns_for_this_oclc_number.append(isbn_identifier)

        # If this OCLC Number didn't tell us about any ISBNs
        # we didn't already know, and there is no description,
        # we don't need to create a WorkRecord for it--it's
        # redundant.
        if (len(new_isbns_for_this_oclc_number) == 0
            and not len(edition['descriptions'])):
            return None, [], [], []

        # Identify the OCLC Number with the OCLC Work.
        w = oclc_work.primarily_identifies
        if w:
            # How similar is the title of the edition to the title of
            # the work, and how much overlap is there between the
            # listed authors?
            oclc_work_record = w[0]
            if title:
                title_strength = MetadataSimilarity.title_similarity(
                    title, oclc_work_record.title)
            else:
                title_strength = 0
            oclc_work_viafs = set([c.viaf for c in oclc_work_record.contributors
                                   if c.viaf])
            author_strength = MetadataSimilarity._proportion(
                oclc_work_viafs, set(edition['creator_viafs']))
            strength = (title_strength * 0.8) + (author_strength * 0.2)
        else:
            strength = 1

        oclc_work.equivalent_to(
            self.oclc_linked_data, oclc_number, strength)

        # Associate all newly created ISBNs with the OCLC
        # Number.
        for isbn_identifier in new_isbns_for_this_oclc_number:
            oclc_number.equivalent_to(
                self.oclc_linked_data, isbn_identifier, 1)

        # Create a description resource for every description.  When
        # there's more than one description for a given edition, only
        # one of them is actually a description. The others are tables
        # of contents or some other stuff we don't need. Unfortunately
        # I can't think of an automatic way to tell which is the good
        # description.
        description_resources = []
        for description in edition['descriptions']:
            description_resource, new = oclc_number.add_resource(
                Resource.DESCRIPTION, None, self.oclc_linked_data,
                content=description)
            description_resources.append(description_resource)

        ld_wr = None
        return ld_wr, new_isbns_for_this_oclc_number, description_resources, classifications

    SEEN_TAGS = set([])

    def info_for(self, work_identifier):
        for data in self.graphs_for(work_identifier):
            subgraph = oclc_linked_data.graph(data)
            for book in oclc_linked_data.books(subgraph):
                info = self.info_for_book_graph(subgraph, book)
                if info:
                    yield info

    TAG_BLACKLIST = set([
        'audiobook', 'audio book', 'large type', 'large print',
        'sound recording', 'compact disc', 'talking book',
        '(binding)', 'movable books', 'electronic books',
    ])

    def fix_tag(self, tag):
        if tag.endswith('.'):
            tag = tag[:-1]
        if tag in self.TAG_BLACKLIST:
            return None
        l = tag.lower()
        if any([x in l for x in self.TAG_BLACKLIST]):
            return None
        return tag

    def info_for_book_graph(self, subgraph, book):
        isbns = set([])
        descriptions = []

        types = []
        type_objs = book.get('rdf:type', [])
        if isinstance(type_objs, dict):
            type_objs = [type_objs]
        types = [i['@id'] for i in type_objs if 
                 i['@id'] not in self.UNUSED_TYPES]
        if not types:
            # This book is not available in any format we're
            # interested in from a metadata perspective.
            return None

        (oclc_id_type,
         oclc_id,
         titles,
         descriptions,
         subjects,
         creator_uris,
         publisher_uris,
         publication_dates,
         example_uris) = OCLCLinkedData.extract_useful_data(subgraph, book)

        example_graphs = OCLCLinkedData.internal_lookup(
            subgraph, example_uris)
        for example in example_graphs:
            for isbn in ldq.values(example.get('schema:isbn', [])):
                if len(isbn) == 10:
                    isbn = isbnlib.to_isbn13(isbn)
                elif len(isbn) != 13:
                    continue
                if isbn:
                    isbns.add(isbn)

        # Consolidate subjects and apply a blacklist.
        tags = set()
        for tag in subjects.get(Subject.TAG, []):
            fixed = self.fix_tag(tag)
            if fixed:
                tags.add(fixed)
        if tags:
            subjects[Subject.TAG] = tags
        elif Subject.TAG in subjects:
            del subjects[Subject.TAG]

        for tag in subjects.get(Subject.TAG, []):
            if not tag in self.SEEN_TAGS:
                print tag
                self.SEEN_TAGS.add(tag)
        # Something interesting has to come out of this
        # work--something we couldn't get from another source--or
        # there's no point.
        if not isbns and not descriptions and not subjects:
            return None

        publishers = OCLCLinkedData.internal_lookup(
            subgraph, publisher_uris)
        publisher_names = [
            i['schema:name'] for i in publishers
            if 'schema:name' in i]
        publisher_names = list(ldq.values(
            ldq.restrict_to_language(publisher_names, 'en')))

        for n in publisher_names:
            if (n in self.PUBLISHER_BLACKLIST
                or 'Audio' in n or 'Video' in n or 'n Tape' in n
                or 'Comic' in n or 'Music' in n):
                # This book is from a publisher that will probably not
                # give us metadata we can use.
                return None

        # Project Gutenberg texts don't have ISBNs, so if there's an
        # ISBN on there, it's probably wrong. Unless someone stuck a
        # description on there, there's no point in discussing
        # OCLC+LD's view of a Project Gutenberg work.
        if ('Project Gutenberg' in publisher_names and not descriptions):
            return None

        creator_viafs = []
        for uri in creator_uris:
            if not uri.startswith("http://viaf.org"):
                continue
            viaf = uri[uri.rindex('/')+1:]
            creator_viafs.append(viaf)

        r = dict(
            oclc_id_type=oclc_id_type,
            oclc_id=oclc_id,
            titles=titles,
            descriptions=descriptions,
            subjects=subjects,
            creator_viafs=creator_viafs,
            publishers=publisher_names,
            publication_dates=publication_dates,
            types=types,
            isbns=isbns,
        )
        return r

    def graphs_for(self, work_identifier):
        data, cached = oclc_linked_data.lookup(work_identifier)
        graph = oclc_linked_data.graph(data)
        examples = oclc_linked_data.extract_workexamples(graph)
        for uri in examples:
            data, cached = oclc_linked_data.lookup(uri)
            yield data
