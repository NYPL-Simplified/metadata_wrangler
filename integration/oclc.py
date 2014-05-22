import collections
import datetime
import os
import md5
from nose.tools import set_trace
import requests
import time
import urllib
from integration import XMLParser
from integration import FilesystemCache
from model import (
    get_one_or_create,
    WorkIdentifier,
    WorkRecord,
    DataSource,
    SubjectType,
)
from lxml import etree


class OCLC(object):
    """Repository for OCLC-related constants."""
    EDITION_COUNT = "OCLC.editionCount"
    HOLDING_COUNT = "OCLC.holdings"
    FORMAT = "OCLC.format"


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
        print " Cache key: %s" % cache_key
        if self.cache.exists(cache_key):
            # Don't go over the wire. Get the raw XML from cache
            # and process it fresh.
            raw = self.cache.open(cache_key).read()
            print " Retrieved from cache."
        else:
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
    LISTS = set([WorkRecord.languages, WorkRecord.authors])

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
    def parse(cls, _db, xml):
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
            print "Extracting work and editions."
            # The representation lists a single work, its editions,
            # plus summary classification information for the work.
            work_tag = cls._xpath1(tree, "//oclc:work")
            work_record, ignore = cls.extract_work_record(_db, work_tag)
            records = [work_record]
            
            for edition_tag in cls._xpath(work_tag, '//oclc:edition'):
                edition_record, ignore = cls.extract_edition_record(_db, edition_tag)
                records.append(edition_record)
                # We identify the edition with the work based on its primary identifier.
                work_record.equivalent_identifiers.append(edition_record.primary_identifier)
                edition_record.equivalent_identifiers.append(work_record.primary_identifier)
        elif representation_type == cls.MULTI_WORK_STATUS:
            # The representation lists a set of works that match the
            # search query.
            print "Extracting SWIDs from search results."
            records = cls.extract_swids(tree)
        elif representation_type == cls.NOT_FOUND_STATUS:
            # No problem; OCLC just doesn't have any data.
            records = []
        else:
            raise IOError("Unrecognized status code from OCLC API: %s (%s)" % (
                representation_type, xml))

        return representation_type, records

    @classmethod
    def extract_swids(cls, tree):
        """Turn a multi-work response into a list of SWIDs."""

        swids = []
        for work_tag in cls._xpath(tree, "//oclc:work"):
            swids.append(work_tag.get('swid'))
        return swids

    @classmethod
    def _parse_author_string(cls, author_string):
        authors = []
        if not author_string:
            return authors
        for author in author_string.split("|"):            
            # TODO: Separate out roles, e.g. "Kent, Rockwell, 1882-1971 [Illustrator]"
            # TODO: Separate out lifespan when possible
            WorkRecord._add_author(authors, author.strip())
        return authors

    @classmethod
    def _extract_basic_info(cls, tag):
        """Extract information common to work tag and edition tag."""
        title = tag.get('title')
        author_string = tag.get('author')
        authors = cls._parse_author_string(author_string)
        # TODO: convert ISO-639-2 to ISO-639-1
        languages = []
        if 'language' in tag.keys():
            languages.append(tag.get('language'))
        return title, authors, languages

    @classmethod
    def extract_work_record(cls, _db, work_tag):
        """Create a new WorkRecord object with information about a
        work (identified by OCLC Work ID).
        """
        oclc_work_id = unicode(work_tag.get('pswid'))
        if not oclc_work_id:
            print " No OCLC Work ID (pswid) in %s" % etree.tostring(work_tag)

        title, authors, languages = cls._extract_basic_info(work_tag)
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
        work_record, new = get_one_or_create(
            _db, WorkRecord,
            data_source=DataSource.lookup(_db, DataSource.OCLC),
            primary_identifier=identifier,
            create_method_kwargs=dict(
                title=title,
                authors=authors,
                languages=languages,
                subjects=subjects,
                extra=extra,
            )
        )
        return work_record, new

    @classmethod
    def extract_edition_record(cls, _db, edition_tag):
        """Create a new WorkRecord object with information about an
        edition of a book (identified by OCLC Number).
        """
        oclc_number = unicode(edition_tag.get('oclc'))

        # Fill in some basic information about this new record.
        title, authors, languages = cls._extract_basic_info(edition_tag)

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
                authors=authors,
                languages=languages,
                subjects=subjects,
                extra=extra,
            )
        )
        return edition_record, new
