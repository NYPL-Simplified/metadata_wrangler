import collections
import datetime
import md5
import os
import re
import requests
import time
import urllib

from pyld import jsonld
from lxml import etree
from nose.tools import set_trace

from integration import XMLParser
from integration import FilesystemCache
from model import (
    Contributor,
    get_one_or_create,
    WorkIdentifier,
    WorkRecord,
    DataSource,
    SubjectType,
)
from util import MetadataSimilarity


class OCLC(object):
    """Repository for OCLC-related constants."""
    EDITION_COUNT = "OCLC.editionCount"
    HOLDING_COUNT = "OCLC.holdings"
    FORMAT = "OCLC.format"

class OCLCLinkedData(object):

    BASE_URL = 'http://experiment.worldcat.org/%(type)s/%(id)s.jsonld'

    def __init__(self, data_directory):
        self.cache_directory = os.path.join(
            data_directory, DataSource.OCLC_LINKED_DATA, "cache")
        self.cache = FilesystemCache(self.cache_directory)

    def cache_key(self, id, type):
        return "%s-%s" % (type, id) + ".jsonld"

    def request(self, url):
        """Make a request to OCLC Linked Data."""
        data = jsonld.load_document(url)
        set_trace()
        #content = response.content
        #if response.status_code != 200:
        #    raise IOError("OCLC Linked Data returned status code %s: %s" % (response.status_code, response.content))
        return content

    def lookup(self, id, type=None):
        """Perform an OCLC Open Data lookup."""
        type = type or "oclc"
        cache_key = self.cache_key(id, type)
        raw = None
        cached = False
        #if self.cache.exists(cache_key):
        #    # Don't go over the wire. Get the raw XML from cache
        #    # and process it fresh.
        #    raw = self.cache.open(cache_key).read()
        #    cached = True
        #    print " Retrieved from cache."
        if not raw:
            url = self.BASE_URL % dict(id=id, type=type)
            print "Requesting %s" % url
            raw = self.request(url)
            print " Retrieved over the net."
            self.cache.store(cache_key, raw)
        return raw, cached


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
        if response.status_code != 200:
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
            raw = self.request(url)
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
        if response.status_code != 200:
            raise IOError("OCLC API returned status code %s: %s" % (response.status_code, response.content))
        return content

    def lookup_by(self, **kwargs):
        """Perform an OCLC lookup."""
        query_string = self.query_string(**kwargs)
        cache_key = self.cache_key(**kwargs)
        print " Query string: %s" % query_string
        raw = None
        if self.cache.exists(cache_key):
            # Don't go over the wire. Get the raw XML from cache
            # and process it fresh.
            raw = self.cache.open(cache_key).read()
        if not raw:
            url = self.BASE_URL + query_string + self.NO_SUMMARY
            raw = self.request(url)
            print " Retrieved over the net."
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
    def _xpath(cls, tag, expression):
        """Wrapper to do a namespaced XPath expression."""
        return tag.xpath(expression, namespaces=cls.NAMESPACES)

    @classmethod
    def _xpath1(cls, tag, expression):
        """Wrapper to do a namespaced XPath expression."""
        values = cls._xpath(tag, expression)
        if not values:
            return None
        return values[0]

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
            raise IOError("Invalid input to OCLC API: %s" % xml)
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

            data_source = DataSource.lookup(_db, DataSource.OCLC)
            for edition_tag in cls._xpath(work_tag, '//oclc:edition'):
                edition_record, ignore = cls.extract_edition_record(
                    _db, edition_tag, existing_authors, **restrictions)
                if not edition_record:
                    # This edition did not become a WorkRecord because it
                    # didn't meet one of the restrictions.
                    continue
                records.append(edition_record)
                # Identify the edition with the work based on its
                # primary identifier.
                work_record.primary_identifier.equivalent_to(
                    data_source, edition_record.primary_identifier)
                edition_record.primary_identifier.equivalent_to(
                    data_source, work_record.primary_identifier)
        elif representation_type == cls.MULTI_WORK_STATUS:
            # The representation lists a set of works that match the
            # search query.
            print "Extracting SWIDs from search results."
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
            languages = [tag.get('language')]
        else:
            languages = None

        if title and 'title' in restrictions:
            must_resemble_title = restrictions['title']
            threshold = restrictions.get('title_similarity', 0.25)
            if MetadataSimilarity.title_similarity(
                    must_resemble_title, title) < threshold:
                # The title of the book under consideration is not
                # similar enough to the given title.
                return None

            # The semicolon is frequently used to separate multiple
            # works in an anthology. If there is no semicolon in the
            # original title, do not consider titles that contain
            # semicolons.
            if (not ';' in must_resemble_title
                and ';' in title and threshold > 0):
                return None

        # Apply restrictions. If they're not met, return None.
        if 'languages' in restrictions and languages:
            # We know which language this record is for. Match it
            # against the language used in the WorkRecord we're
            # matching against.
            restrict_to_languages = set(restrictions['languages'])
            if not restrict_to_languages.intersection(languages):
                # This record is for a book in a different language
                return None

        if 'authors' in restrictions:
            restrict_to_authors = restrictions['authors']
            authors_per_se = [
                a for a, roles in authors_and_roles if Contributor.AUTHOR_ROLE in roles
            ]
            for restrict_to_author in restrict_to_authors:
                if not restrict_to_author in authors_per_se:
                    # The given author did not show up as one of the
                    # per se 'authors' of this book. They may have had
                    # some other role in it, or the book may be about
                    # them, but this book is not *by* them.
                    return None

        return title, authors_and_roles, languages

    @classmethod
    def extract_work_record(cls, _db, work_tag, existing_authors, **restrictions):
        """Create a new WorkRecord object with information about a
        work (identified by OCLC Work ID).
        """
        oclc_work_id = unicode(work_tag.get('pswid'))
        if oclc_work_id:
            print " owi: %s" % oclc_work_id
        else:
            print " No owi in %s" % etree.tostring(work_tag)


        try:
            int(oclc_work_id)
        except ValueError, e:
            # This record does not have a valid OCLC Work ID.
            return None, False

        result = cls._extract_basic_info(_db, work_tag, existing_authors, **restrictions)
        if not result:
            # This record did not meet one of the restrictions.
            return None, False

        title, authors_and_roles, languages = result


        # Get the most popular Dewey and LCC classification for this
        # work.
        subjects = {}
        for tag_name, subject_type in (
                ("ddc", SubjectType.DDC),
                ("lcc", SubjectType.LCC)):
            tag = cls._xpath1(
                work_tag,
                "//oclc:%s/oclc:mostPopular" % tag_name)
            if tag is not None:
                id = tag.get('nsfa') or tag.get('sfa')
                weight = int(tag.get('holdings'))
                WorkRecord._add_subject(subjects, subject_type, id, weight=weight)

        # Find FAST subjects for the work.
        for heading in cls._xpath(
                work_tag, "//oclc:fast//oclc:heading"):
            id = heading.get('ident')
            weight = int(heading.get('heldby'))
            value = heading.text
            WorkRecord._add_subject(subjects, SubjectType.FAST, id, value, weight=weight)

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
                languages=languages,
                subjects=subjects,
                extra=extra,
            )
        )

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

        title, authors_and_roles, languages = result

        subjects = {}
        for subject_type, oclc_code in (
                (SubjectType.LCC, "050"),
                (SubjectType.DDC, "082")):
            classification = cls._xpath1(edition_tag,
                "oclc:classifications/oclc:class[@tag=%s]" % oclc_code)
            if classification is not None:
                value = classification.get("nsfa") or classification.get('sfa')
                WorkRecord._add_subject(subjects, subject_type, value)

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
        edition_record, new = get_one_or_create(
            _db, WorkRecord,
            data_source=DataSource.lookup(_db, DataSource.OCLC),
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                languages=languages,
                subjects=subjects,
                extra=extra,
            )
        )

        # Associated each contributor with the new record.
        for author, roles in authors_and_roles:
            edition_record.add_contributor(author, roles)
        return edition_record, new


class OCLCLinkedDataParser(object):

    @classmethod
    def parse(_db, json_data):
        """Turn JSON-LD data from OCLC into a WorkRecord object.

        Will also create a bunch of Equivalencies.
        """
        
