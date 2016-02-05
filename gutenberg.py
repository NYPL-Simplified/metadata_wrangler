import re
from nose.tools import set_trace

from core.coverage import CoverageProvider
from core.model import DataSource
from oclc import (
    OCLCClassifyAPI,
    OCLCXMLParser,
)

class OCLCClassifyCoverageProvider(CoverageProvider):
    """Does title/author lookups using OCLC Classify."""

    # Strips most non-alphanumerics from the title.
    # 'Alphanumerics' includes alphanumeric characters
    # for any language, so this shouldn't affect
    # titles in non-Latin languages.
    #
    # OCLC has trouble recognizing non-alphanumerics in titles,
    # especially colons.
    NON_TITLE_SAFE = re.compile("[^\w\-' ]", re.UNICODE)
    
    def __init__(self, _db, input_source_name):
        self._db = _db
        self.oclc_classify = OCLCClassifyAPI(self._db)
        input_source = DataSource.lookup(self._db, input_source_name)
        output_source = DataSource.lookup(self._db, DataSource.OCLC)
        super(OCLCClassifyCoverageProvider, self).__init__(
            "OCLC Classify Monitor for %s" % input_source.name,
            input_source, output_source)

    def oclc_safe_title(self, title):
        return self.NON_TITLE_SAFE.sub("", title)

    def title_and_author(self, book):
        title = self.oclc_safe_title(book.title)

        authors = book.author_contributors
        if len(authors) == 0:
            author = ''
        else:
            author = authors[0].name
        return title, author

    def process_edition(self, book):
        title, author = self.title_and_author(book)
        language = book.language

        def _f(s):
            if not s:
                return ''
            if isinstance(s, unicode):
                return s.encode("utf8")
            return s
        self.log.info('%s "%s" "%s" %r', _f(book.primary_identifier.identifier), _f(title), _f(author), _f(language))
        # Perform a title/author lookup
        xml = self.oclc_classify.lookup_by(title=title, author=author)

        # For now, the only restriction we apply is the language
        # restriction. If we know that a given OCLC record is in a
        # different language from this record, there's no need to
        # even import that record. Restrictions on title and
        # author will be applied statistically, when we calculate
        # works.
        restrictions = dict(language=language,
                            title=title,
                            authors=book.author_contributors)

        # Turn the raw XML into some number of bibliographic records.
        representation_type, records = OCLCXMLParser.parse(
            self._db, xml, **restrictions)

        if representation_type == OCLCXMLParser.MULTI_WORK_STATUS:
            # `records` contains a bunch of SWIDs, not
            # Editions. Do another lookup to turn each SWID
            # into a set of Editions.
            swids = records
            records = []
            for swid in swids:
                swid_xml = self.oclc_classify.lookup_by(wi=swid)
                representation_type, editions = OCLCXMLParser.parse(
                    self._db, swid_xml, **restrictions)

                if representation_type == OCLCXMLParser.SINGLE_WORK_DETAIL_STATUS:
                    records.extend(editions)
                elif representation_type == OCLCXMLParser.NOT_FOUND_STATUS:
                    # This shouldn't happen, but if it does,
                    # it's not a big deal. Just do nothing.
                    pass
                elif representation_type == OCLCXMLParser.INVALID_INPUT_STATUS:
                    # This also shouldn't happen, but if it does,
                    # there's nothing we can do.
                    pass                    
                elif representation_type == OCLCXMLParser.NO_INPUT_STATUS:
                    # This _really_ shouldn't happen, but if it does,
                    # there's nothing we can do.                    
                    pass
                else:
                    raise IOError("Got unexpected representation type from lookup: %s" % representation_type)
        # Connect the Gutenberg book to the OCLC works looked up by
        # title/author. Hopefully we can also connect the Gutenberg book
        # to an author who has an LC and VIAF.

        # First, find any authors associated with this book that
        # have not been given VIAF or LC IDs.
        gutenberg_authors_to_merge = [
            x for x in book.author_contributors if not x.viaf or not x.lc
        ]
        gutenberg_names = set([x.name for x in book.author_contributors])
        for r in records:
            if gutenberg_authors_to_merge:
                oclc_names = set([x.name for x in r.author_contributors])
                if gutenberg_names == oclc_names:
                    # Perfect overlap. We've found an OCLC record
                    # for a book written by exactly the same
                    # people as the Gutenberg book. Merge each
                    # Gutenberg author into its OCLC equivalent.
                    for gutenberg_author in gutenberg_authors_to_merge:
                        oclc_authors = [x for x in r.author_contributors
                                        if x.name==gutenberg_author.name]
                        if len(oclc_authors) == 1:
                            oclc_author = oclc_authors[0]
                            if oclc_author != gutenberg_author:
                                gutenberg_author.merge_into(oclc_author)
                                gutenberg_authors_to_merge.remove(
                                    gutenberg_author)

            # Now that we've (perhaps) merged authors, calculate the
            # similarity between the two records.
            strength = book.similarity_to(r)
            if strength > 0:
                book.primary_identifier.equivalent_to(
                    self.output_source, r.primary_identifier, strength)

        self.log.info("Created %s records(s).", len(records))
        return True
