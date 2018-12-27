# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
from core.coverage import CoverageFailure
from core.model import Contributor, Identifier, Measurement
from core.metadata_layer import *
from oclc.classify import (
    IdentifierLookupCoverageProvider,
    OCLCClassifyXMLParser,
)

class MockParser(OCLCClassifyXMLParser):
    def __init__(self):
        self.call_count = 0
        self.called_with = []

    def parse(self, tree, metadata):
        self.call_count += 1
        self.called_with = metadata
        return self.called_with

class MockParserSingle(MockParser):
    def initial_look_up(self, tree):
        return 2, []

class MockParserMulti(MockParser):
    def initial_look_up(self, tree):
        results = []
        owi_numbers = ["48446512", "48525129"]
        for number in owi_numbers:
            data = IdentifierData(Identifier.OCLC_WORK, number)
            results.append(data)
        return 4, results

class MockProvider(IdentifierLookupCoverageProvider):

    def __init__(self, collection):
        self.apply_called_with = []
        super(MockProvider, self).__init__(collection)

    def _single(self, tree, metadata):
        self.called_with = dict(tree=tree, identifier=metadata.primary_identifier)
        metadata.data_source = DataSource.OCLC
        return [metadata]

    def _multiple(self, owi_data, metadata):
        identifier = metadata.primary_identifier
        self.called_with = dict(owi_data=owi_data, identifier=identifier)
        return([
            Metadata(identifiers=[identifier, owi_data[0]], data_source=DataSource.OCLC),
            Metadata(identifiers=[identifier, owi_data[1]], data_source=DataSource.OCLC)
        ])

    def _apply(self, metadata):
        identifiers = [x.identifiers for x in metadata]
        self.apply_called_with += identifiers

class MockProviderSingle(MockProvider):
    def _get_tree(self, **kwargs):
        xml = sample_data("single_work_with_isbn.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class MockProviderMulti(MockProvider):
    def _get_tree(self, **kwargs):
        xml = sample_data("multi_work_with_owis.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class TestIdentifierLookupCoverageProvider(DatabaseTest):

    SINGLE_ISBN = "9781620281932"
    MULTI_ISBN = "0345391837"

    def _tree(self, type):
        if type == "single":
            return MockProviderSingle(self._default_collection)._get_tree(isbn=self.SINGLE_ISBN)
        else:
            return MockProviderMulti(self._default_collection)._get_tree(isbn=self.MULTI_ISBN)

    def _id(self, type):
        if type == "single":
            return self._identifier(Identifier.ISBN, self.SINGLE_ISBN)
        else:
            return self._identifier(Identifier.ISBN, self.MULTI_ISBN)

    def test_process_item_single(self):
        # Testing that, when process_item finds out that a document's status code is 2,
        # it calls _single, passes in the correct tree and identifier as arguments,
        # and returns the original ISBN.  Uses mocked versions of _get_tree,
        # initial_look_up, and _single.

        provider = MockProviderSingle(self._default_collection)
        provider.parser = MockParserSingle()
        id = self._id("single")
        result = provider.process_item(id)
        eq_(etree.tostring(provider.called_with["tree"]), etree.tostring(provider._get_tree(isbn=self.SINGLE_ISBN)))
        eq_(provider.called_with["identifier"], id)
        eq_(result, id)

    def test_process_item_multi(self):
        # Testing that, when process_item finds out that a document's status code is 4,
        # it calls _multiple, passes in the correct OWIs, and
        # returns the original ISBN.  Uses mocked versions of _get_tree, initial_look_up,
        # and _multiple.

        provider = MockProviderMulti(self._default_collection)
        provider.parser = MockParserMulti()
        id = self._id("multi")

        result = provider.process_item(id)
        eq_([x.identifier for x in provider.called_with["owi_data"]], ["48446512", "48525129"])
        eq_(provider.called_with["identifier"], id)
        eq_(result, id)

    def test_process_item_failure(self):
        # If the ISBN is not found--i.e. the status code is 102--the provider should throw an error.
        provider = IdentifierLookupCoverageProvider(self._default_collection)
        bad_id = self._identifier(Identifier.ISBN, "9781429984171")
        failure = provider.process_item(bad_id)
        assert isinstance(failure, CoverageFailure)
        eq_(failure.exception, "The work with ISBN 9781429984171 was not found.")

    def test__apply_single(self):
        # Testing that, in the case of a single-work response, _apply is called with the return value of _single.
        provider = MockProviderSingle(self._default_collection)
        provider.parser = MockParserSingle()
        id = self._id("single")
        provider.process_item(id)

        [result] = provider.apply_called_with[0]
        eq_(result.identifier, id.identifier)

    def test__apply_multiple(self):
        # Testing that, in the case of a multi-work response, _apply is called for each of the results from _multiple.
        provider = MockProviderMulti(self._default_collection)
        provider.parser = MockParserMulti()
        id = self._id("multi")
        provider.process_item(id)

        results = provider.apply_called_with
        eq_(len(results), 2)

        isbn_1, owi_1 = results[0]
        eq_(isbn_1.identifier, id.identifier)
        eq_(owi_1.identifier, "48446512")

        isbn_2, owi_2 = results[1]
        eq_(isbn_2.identifier, id.identifier)
        eq_(owi_2.identifier, "48525129")

    def test__single(self):
        # Testing that _single calls parse, passes in the correct tree and
        # identifier as arguments, and returns the resulting value.  Uses a mocked
        # version of parse.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        provider.parser = MockParserSingle()
        tree, identifier = self._tree("single"), self._id("single")
        metadata = self._blank_metadata(identifier)

        provider._single(tree, metadata)
        result = provider.parser.called_with

        eq_((result.primary_identifier.type, result.primary_identifier.identifier), (Identifier.ISBN, self.SINGLE_ISBN))

    def test__multiple(self):
        # Testing that _multiple calls parse, passes in the correct OWIs, and
        # returns the resulting value.  Uses mocked versions of
        # initial_look_up (to get the list of OWIs) and parse.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        provider.parser = MockParserMulti()
        tree, identifier = self._tree("multi"), self._id("multi")
        metadata = self._blank_metadata(identifier)

        code, owi_data = provider.parser.initial_look_up(tree)
        provider._multiple(owi_data, metadata)
        result = provider.parser.called_with
        # Make sure parse was called twice--once for each of the two OWIs.
        eq_(provider.parser.call_count, 2)

        eq_(result.primary_identifier.identifier, self.MULTI_ISBN)
        assert isinstance(result.primary_identifier, Identifier)

    def test__single_with_real_parser(self):
        # Testing that calling _single actually returns the correct metadata object.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        tree, identifier = self._tree("single"), self._id("single")
        metadata = self._blank_metadata(identifier)
        result = provider._single(tree, metadata)

        assert isinstance(result, Metadata)
        eq_(result._data_source, "OCLC Classify")
        eq_(result.primary_identifier, identifier)
        self._check_measurements(result.measurements, "single")

        [author] = result.contributors
        assert isinstance(author, ContributorData)
        eq_(self._get_contributor_info(author), ("Melville, Herman", "n79006936", "27068555", ["Author"], {"deathDate": "1891", "birthDate": "1819"}))

    def test__multiple_with_real_parser(self):
        # Testing that calling _multiple actually returns the correct metadata objects.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        tree, identifier = self._tree("multi"), self._id("multi")
        metadata = self._blank_metadata(identifier)
        code, owi_data = provider.parser.initial_look_up(tree)
        result = provider._multiple(owi_data, metadata)
        # The document contained two <work> tags and therefore two OWIs, so we
        # end up with two results.  They should both be Metadata objects, and
        # should have the same data source and ISBN.

        assert isinstance(result, Metadata)
        eq_(result._data_source, "OCLC Classify")
        eq_(result.primary_identifier, identifier)

        # The author and measurement info just comes from the first work.
        expected_author_info = ("Adams, Douglas", "n80076765", "113230702", ["Author"], {"deathDate": "2001", "birthDate": "1952"})
        [author] = result.contributors
        author_info = self._get_contributor_info(author)
        eq_(author_info, expected_author_info)

        self._check_measurements(result.measurements, "multi")

        # The subject data is collected from both works.
        [ddc], [lcc], fast = self._get_subjects(result.subjects)
        eq_(ddc.identifier, "823.914")
        eq_(lcc.identifier, "PR6051.D3352")
        eq_(len(fast), 7)
        eq_(set([x.identifier for x in fast]), set(['890366', '1075077', '977455', '977550', '923709', '963836', '1108670']))

    def _get_subjects(self, subjects):
        # Everything in the list of subjects should be a SubjectData object.
        eq_(len([x for x in subjects if isinstance(x, SubjectData)]), len(subjects))
        # Extract a sublist for each type of classifier.
        sublists = [[x for x in subjects if x.type == type] for type in ["DDC", "LCC", "FAST"]]
        # There should always be 1 DDC classification and 1 LCC classification.
        eq_((len(sublists[0]), len(sublists[1])), (1, 1))
        return sublists

    def _check_measurements(self, measurements, type):
        values = {
            "single": {
                Measurement.HOLDINGS: 41932,
                Measurement.PUBLISHED_EDITIONS: 1
            },
            "multi": {
                Measurement.HOLDINGS: 5956,
                Measurement.PUBLISHED_EDITIONS: 153
            }
        }
        eq_(len(measurements), 2)
        [holdings], [editions] = [[x for x in measurements if y in x.quantity_measured] for y in ["holdings", "editions"]]
        for m in [holdings, editions]:
            assert isinstance(m, MeasurementData)
            expected_value = values[type][m.quantity_measured]
            eq_(m.weight, 1)
            eq_(m.value, expected_value)

    def _get_contributor_info(self, contributor):
        return (
            contributor.sort_name,
            contributor.lc,
            contributor.viaf,
            contributor.roles,
            contributor.extra
        )

    def _blank_metadata(self, identifier):
        metadata = Metadata(
            data_source=DataSource.OCLC,
            primary_identifier=identifier
        )
        return metadata
