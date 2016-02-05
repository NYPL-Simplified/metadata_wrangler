import re
from nose.tools import set_trace

from core.coverage import (
    CoverageProvider,
    CoverageFailure,
)
from core.model import (
    DataSource,
    Identifier,
)
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

    def __init__(self, _db):
        self._db = _db
        self.api = OCLCClassifyAPI(self._db)
        input_identifier_types = [
            Identifier.THREEM_ID, Identifier.GUTENBERG_ID, Identifier.URI
        ]
        output_source = DataSource.lookup(self._db, DataSource.OCLC)
        super(OCLCClassifyCoverageProvider, self).__init__(
            "OCLC Classify Coverage Provider", input_identifier_types,
            output_source)

    def oclc_safe_title(self, title):
        return self.NON_TITLE_SAFE.sub("", title)

    def get_edition_info(self, edition):
        """Returns the API-safe title, author(s), and language for an
        edition
        """
        title = self.oclc_safe_title(edition.title)

        authors = edition.author_contributors
        if len(authors) == 0:
            author = ''
        else:
            author = authors[0].name

        language = edition.language

        # Log the info
        def _f(s):
            if not s:
                return ''
            if isinstance(s, unicode):
                return s.encode("utf8")
            return s
        self.log.info(
            '%s "%s" "%s" %r', _f(edition.primary_identifier.identifier),
            _f(title), _f(author), _f(language)
        )

        return title, author, language

    def parse_edition_data(self, xml, edition, title, language):
        """Transforms the OCLC XML files into usable bibliographic records,
        including making additional API calls as necessary.
        """
        parser = OCLCXMLParser()
        # For now, the only restriction we apply is the language
        # restriction. If we know that a given OCLC record is in a
        # different language from this record, there's no need to
        # even import that record. Restrictions on title and
        # author will be applied statistically, when we calculate
        # works.
        restrictions = dict(language=language,
                            title=title,
                            authors=edition.author_contributors)
        # These representation types shouldn't occur, but if they do there's
        # either nothing we need to do about them or nothing we can do.
        ignored_representation_types = [
            parser.NOT_FOUND_STATUS, parser.INVALID_INPUT_STATUS,
            parser.NO_INPUT_STATUS
        ]

        # Turn the raw XML into some number of bibliographic records.
        representation_type, records = parser.parse(
            self._db, xml, **restrictions
        )

        if representation_type == parser.MULTI_WORK_STATUS:
            # `records` contains a bunch of SWIDs, not
            # Editions. Do another lookup to turn each SWID
            # into a set of Editions.
            swids = records
            records = []
            for swid in swids:
                swid_xml = self.api.lookup_by(wi=swid)
                representation_type, editions = parser.parse(
                    self._db, swid_xml, **restrictions
                )
                if representation_type == parser.SINGLE_WORK_DETAIL_STATUS:
                    records.extend(editions)
                elif representation_type in ignored_representation_types:
                    pass
                else:
                    raise IOError(
                        "Got unexpected representation type from \
                        lookup: %s" % representation_type
                    )
        return records

    def merge_contributors(self, edition, records):
        """Connect the Gutenberg book to the OCLC works looked up by
        title/author. Hopefully we can also connect the Gutenberg book
        to an author who has an LC and VIAF.
        """
        # First, find any authors associated with this book that
        # have not been given VIAF or LC IDs.
        gutenberg_authors_to_merge = [
            x for x in edition.author_contributors if not x.viaf or not x.lc
        ]
        gutenberg_names = set([x.name for x in edition.author_contributors])
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
                                        if x.name == gutenberg_author.name]
                        if len(oclc_authors) == 1:
                            oclc_author = oclc_authors[0]
                            if oclc_author != gutenberg_author:
                                gutenberg_author.merge_into(oclc_author)
                                gutenberg_authors_to_merge.remove(
                                    gutenberg_author)

            # Now that we've (perhaps) merged authors, calculate the
            # similarity between the two records.
            strength = edition.similarity_to(r)
            if strength > 0:
                edition.primary_identifier.equivalent_to(
                    self.output_source, r.primary_identifier, strength
                )

    def process_item(self, identifier):
        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition

        # Perform a title/author lookup.
        title, author, language = self.get_edition_info(edition)
        xml = self.api.lookup_by(title=title, author=author)
        try:
            records = self.parse_edition_data(xml, edition, title, language)
        except IOError as e:
            return CoverageFailure(self, identifier, e)

        self.merge_contributors(edition, records)
        self.log.info("Created %s records(s).", len(records))
        return identifier
