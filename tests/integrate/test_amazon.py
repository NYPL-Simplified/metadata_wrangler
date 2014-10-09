# encoding: utf-8
from nose.tools import set_trace, eq_
import pkgutil

from integration.amazon import (
    AmazonBibliographicParser,
    AmazonReviewParser,
)

from model import (
    Identifier,
    Measurement,
)

class TestBibliographicParser(object):

    def bib(self, filename):
        data = pkgutil.get_data(
            "tests.integrate",
            "files/amazon/%s" % filename)
        return AmazonBibliographicParser().process_all(data)

    def test_basic_kindle(self):
        bib = self.bib("kindle_keywords_and_similar_items.html")

        # We found the title.
        eq_("Constellation Games", bib['title'])

        # We couldn't find any equivalent identifiers.
        eq_([], bib['identifiers'])

        # We took three measurements
        measurements = bib['measurements']
        eq_(4.4, measurements[Measurement.RATING])
        eq_(156368, measurements[Measurement.POPULARITY])
        eq_(480, measurements[Measurement.PAGE_COUNT])

        # We found a number of keywords.
        keywords = bib['keywords']
        assert 'FICTION / Humorous' in keywords
        assert 'Science Fiction & Fantasy' in keywords

        # The title is filtered from keywords, along with non-useful
        # keywords like "Kindle".
        assert "Constellation Games" not in keywords
        assert not any (['kindle' in x.lower() for x in keywords])
        assert not any (['ebook' in x.lower() for x in keywords])

    def test_public_domain(self):
        bib = self.bib("kindle_public_domain.html")
        # We found the title.
        eq_("Anna Karenina", bib['title'])

        # We found the sales rank, even though it's presented
        # differently for public domain books.
        eq_(954, bib['measurements'][Measurement.POPULARITY])

    def test_numbers_filtered_from_keywords(self):
        bib = self.bib("print_no_keyword_but_title.html")
        assert not "" in bib['keywords']
        assert not "1922-1983" in bib['keywords']
        measurements = bib['measurements']
        eq_(4.2, measurements[Measurement.RATING])
        eq_(961013, measurements[Measurement.POPULARITY])
        assert not Measurement.PAGE_COUNT in measurements
        eq_([], bib['identifiers'])

    def test_print_find_identifier(self):
        bib = self.bib("print_keywords_only.html")
        measurements = bib['measurements']
        measurements = bib['measurements']
        eq_(4.3, measurements[Measurement.RATING])
        eq_(400113, measurements[Measurement.POPULARITY])

        # TODO: <li><b>Paperback|Hardcover:</b> 368 pages</li>
        assert not Measurement.PAGE_COUNT in measurements

        # We found one equivalent identifier.
        eq_([(Identifier.ASIN, 'B008RH5I0A')], bib['identifiers'])

