# encoding: utf-8

import json
from nose.tools import set_trace, eq_
from .. import (
    DatabaseTest,
    sample_data
)
from lxml import etree
from core.coverage import CoverageFailure
from core.model import Contributor, Identifier
from core.metadata_layer import *
from oclc.classify import (
    IdentifierLookupCoverageProvider,
    OCLCClassifyXMLParser,
)

class MockParser(OCLCClassifyXMLParser):
    def __init__(self):
        self.call_count = 0
        self.called_with = []

    def parse(self, db, tree, identifiers):
        self.call_count += 1
        self.called_with.append(identifiers)

class MockParserSingle(MockParser):
    def initial_look_up(self, db, tree):
        return 2, []

class MockParserMulti(MockParser):
    def initial_look_up(self, db, tree):
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

    def _single(self, db, tree, identifier):
        self.called_with = dict(tree=tree, identifier=identifier)
        return [Metadata(identifiers=identifier, data_source=DataSource.OCLC)]

    def _multiple(self, db, owi_data, identifier):
        self.called_with = dict(owi_data=owi_data, identifier=identifier)
        return([
            Metadata(identifiers=[identifier, owi_data[0]], data_source=DataSource.OCLC),
            Metadata(identifiers=[identifier, owi_data[1]], data_source=DataSource.OCLC)
        ])

    def _apply(self, metadata, identifier):
        self.apply_called_with.append(metadata.identifiers)

class MockProviderSingle(MockProvider):
    def _get_tree(self, isbn):
        xml = sample_data("single_work_with_isbn.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class MockProviderMulti(MockProvider):
    def _get_tree(self, isbn):
        xml = sample_data("multi_work_with_owis.xml", "oclc_classify")
        return etree.fromstring(xml, parser=etree.XMLParser(recover=True))

class TestIdentifierLookupCoverageProvider(DatabaseTest):

    SINGLE_ISBN = "9781620281932"
    MULTI_ISBN = "0345391837"

    def _tree(self, type):
        if type == "single":
            return MockProviderSingle(self._default_collection)._get_tree(self.SINGLE_ISBN)
        else:
            return MockProviderMulti(self._default_collection)._get_tree(self.MULTI_ISBN)

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
        eq_(etree.tostring(provider.called_with["tree"]), etree.tostring(provider._get_tree(self.SINGLE_ISBN)))
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

    def test__apply_single(self):
        # Testing that, in the case of a single-work response, _apply is called with the return value of _single.
        provider = MockProviderSingle(self._default_collection)
        provider.parser = MockParserSingle()
        id = self._id("single")
        provider.process_item(id)

        [result] = provider.apply_called_with
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

        provider._single(self._db, tree, identifier)[0]
        [result] = provider.parser.called_with[0]

        eq_((result.type, result.identifier), (Identifier.ISBN, self.SINGLE_ISBN))

    def test__multiple(self):
        # Testing that _multiple calls parse, passes in the correct OWIs, and
        # returns the resulting value.  Uses mocked versions of
        # initial_look_up (to get the list of OWIs) and parse.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        provider.parser = MockParserMulti()
        tree, identifier = self._tree("multi"), self._id("multi")

        code, owi_data = provider.parser.initial_look_up(self._db, tree)
        provider._multiple(self._db, owi_data, identifier)
        result_1, result_2 = provider.parser.called_with
        # Make sure parse was called twice--once for each of the two OWIs.
        eq_(provider.parser.call_count, 2)

        # Each result is a list containing one ISBN and one OWI.
        eq_(len(result_1), 2)
        eq_(len(result_2), 2)

        for isbn in [result_1[0], result_2[0]]:
            eq_(isbn.type, Identifier.ISBN)
            eq_(isbn.identifier, self.MULTI_ISBN)
            assert isinstance(isbn, Identifier)

        for idx, owi in enumerate([result_1[1], result_2[1]]):
            eq_(owi.type, Identifier.OCLC_WORK)
            eq_(owi.identifier, owi_data[idx].identifier)
            assert isinstance(owi, IdentifierData)

    def test__single_with_real_parser(self):
        # Testing that calling _single actually returns the correct metadata object.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        tree, identifier = self._tree("single"), self._id("single")
        [result] = provider._single(self._db, tree, identifier)

        assert isinstance(result, Metadata)
        eq_(result._data_source, "OCLC Classify")
        eq_(result.identifiers[0], identifier)

        self._check_measurements(result.measurements, [41932, 1])

        [author] = result.contributors
        assert isinstance(author, ContributorData)
        eq_(self._get_contributor_info(author), ("Melville, Herman", "n79006936", "27068555", ["Author"], {"deathDate": "1891", "birthDate": "1819"}))

    def test__multiple_with_real_parser(self):
        # Testing that calling _multiple actually returns the correct metadata objects.

        provider = IdentifierLookupCoverageProvider(self._default_collection)
        tree, identifier = self._tree("multi"), self._id("multi")
        code, owi_data = provider.parser.initial_look_up(self._db, tree)
        results = provider._multiple(self._db, owi_data, identifier)

        # The document contained two <work> tags and therefore two OWIs, so we
        # end up with two results.  They should both be Metadata objects, and
        # should have the same data source and ISBN.

        eq_(len(results), 2)
        for result in results:
            assert isinstance(result, Metadata)
            eq_(result._data_source, "OCLC Classify")
            eq_(result.identifiers[0], identifier)

        result_1, result_2 = results

        # Result 1:
        eq_(result_1.identifiers[1], owi_data[0])

        expected_author_info = ("Adams, Douglas", "n80076765", "113230702", ["Author"], {"deathDate": "2001", "birthDate": "1952"})
        [author] = result_1.contributors
        author_info = self._get_contributor_info(author)
        eq_(author_info, expected_author_info)

        self._check_measurements(result_1.measurements, [3786, 112])

        [ddc], [lcc], fast = self._get_subjects(result_1.subjects)
        eq_(ddc.identifier, "823.914")
        eq_(lcc.identifier, "PR6051.D3352")
        eq_(len(fast), 5)
        eq_([x.identifier for x in fast], ['890366', '1075077', '977455', '977550', '923709'])

        # Result 2:
        eq_(result_2.identifiers[1], owi_data[1])

        eq_(len(result_2.contributors), 2)
        author_1, author_2 = result_2.contributors

        eq_(self._get_contributor_info(author_1), ("Gaiman, Neil", "n90640849", "103859257", ["Author"], {}))
        eq_(self._get_contributor_info(author_2), expected_author_info)

        self._check_measurements(result_2.measurements, [2170, 41])

        [ddc], [lcc], fast = self._get_subjects(result_2.subjects)
        eq_(ddc.identifier, "823.914")
        eq_(lcc.identifier, "PR6051.D3352")
        eq_(len(fast), 6)
        eq_([x.identifier for x in fast], ['963836', '1108670', '1075077', '890366', '977455', '977550'])

    def _get_subjects(self, subjects):
        # Everything in the list of subjects should be a SubjectData object.
        eq_(len([x for x in subjects if isinstance(x, SubjectData)]), len(subjects))
        # Extract a sublist for each type of classifier.
        sublists = [[x for x in subjects if x.type == type] for type in ["DDC", "LCC", "FAST"]]
        # There should always be 1 DDC classification and 1 LCC classification.
        eq_((len(sublists[0]), len(sublists[1])), (1, 1))
        return sublists

    def _check_measurements(self, measurements, values):
        eq_(len(measurements), 2)
        eq_(measurements[0].quantity_measured, "holdings")
        eq_(measurements[1].quantity_measured, "editions")
        for idx, m in enumerate(measurements):
            assert isinstance(m, MeasurementData)
            eq_(m.weight, 1)
            eq_(m.value, values[idx])

    def _get_contributor_info(self, contributor):
        return (
            contributor.sort_name,
            contributor.lc,
            contributor.viaf,
            contributor.roles,
            contributor.extra
        )
